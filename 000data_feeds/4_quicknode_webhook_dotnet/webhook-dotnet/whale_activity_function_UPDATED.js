function main(payload) {
  // Retain the original raw data for the output
  const { data, metadata } = payload;
  
  // Track timing
  const functionReceivedAt = Date.now();
  const functionReceivedISO = new Date().toISOString();
  
  // WHALE DETECTION THRESHOLDS
  const MIN_WALLET_BALANCE = 1000;  // Wallet must have 1000+ SOL to be considered a whale
  const MIN_SOL_MOVEMENT = 50;      // Movement must be 50+ SOL to be significant
  
  // Known exchange wallets to EXCLUDE (we don't care about exchange movements)
  const EXCLUDED_EXCHANGES = [
    '5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9',  // Binance
    'H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS',  // Coinbase
    'AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2',  // FTX (inactive but still exclude)
    // Add more exchange addresses as you discover them
  ];
  
  // ========== PERP PLATFORM PROGRAM IDs ==========
  const PERP_PROGRAMS = {
    'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH': 'drift',
    'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB': 'jupiter',
    'MangoCzJ36AjZyKwVj3VnYU4GTonjfVEnJmvvWaxLac': 'mango',
    'ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD': 'zeta'
  };
  
  const whaleMovements = [];
  let skippedNotWhale = 0;
  let skippedSmallMovement = 0;
  let skippedExchange = 0;
  
  // ========== IMPROVED: Helper function to detect perp platform interaction ==========
  function detectPerpInteraction(instructions, accountKeys) {
    let perpPlatform = null;
    let perpDirection = null;
    let perpDetails = null;

    try {
      for (const ix of instructions) {
        const programId = ix.programId;
        
        // Check if this instruction involves a perp platform
        if (PERP_PROGRAMS[programId]) {
          perpPlatform = PERP_PROGRAMS[programId];
          
          // Parse the instruction based on platform
          if (perpPlatform === 'drift') {
            const parsed = parseDriftInstruction(ix, accountKeys);
            if (parsed) {
              perpDirection = parsed.direction;
              perpDetails = parsed.details;
              break; // Found a perp interaction, stop searching
            }
          } else if (perpPlatform === 'jupiter') {
            const parsed = parseJupiterPerpInstruction(ix, accountKeys);
            if (parsed) {
              perpDirection = parsed.direction;
              perpDetails = parsed.details;
              break;
            }
          }
          // For other platforms, just detect presence without parsing details
          break;
        }
      }
    } catch (e) {
      console.error('Error detecting perp interaction:', e.message);
    }

    return {
      has_perp_position: perpPlatform !== null,
      perp_platform: perpPlatform,
      perp_direction: perpDirection,
      perp_details: perpDetails
    };
  }

  // ========== IMPROVED: Parse Drift instruction with multiple attempts ==========
  function parseDriftInstruction(instruction, accountKeys) {
    try {
      const data = Buffer.from(instruction.data, 'base64');
      
      // Log the instruction for debugging (first occurrence only)
      if (!parseDriftInstruction.logged) {
        console.log(`📊 Drift instruction data length: ${data.length} bytes`);
        console.log(`📊 First 32 bytes (hex): ${data.slice(0, Math.min(32, data.length)).toString('hex')}`);
        parseDriftInstruction.logged = true;
      }
      
      // Drift instructions have an 8-byte discriminator
      if (data.length < 8) return null;

      // Try multiple parsing approaches since we don't have the real IDL
      
      // ATTEMPT 1: Check for common Drift discriminators
      // OpenPosition, ModifyOrder, PlacePerpOrder, etc.
      const discriminator = data.slice(0, 8);
      const discHex = discriminator.toString('hex');
      
      // Try to infer direction from account keys or data patterns
      // Drift typically passes: user account, state, market, etc.
      
      // ATTEMPT 2: Look for market index in various positions
      for (let offset = 8; offset < Math.min(data.length - 2, 20); offset++) {
        try {
          const marketIndex = data.readUInt16LE(offset);
          
          // SOL-PERP is typically market 0
          if (marketIndex === 0 && offset + 3 < data.length) {
            // Try to read direction from next bytes
            const potentialDirection = data.readUInt8(offset + 2);
            
            if (potentialDirection === 0 || potentialDirection === 1) {
              // Try to read size
              let size = null;
              if (data.length >= offset + 11) {
                try {
                  const sizeLow = data.readUInt32LE(offset + 3);
                  const sizeHigh = data.readInt32LE(offset + 7);
                  const rawSize = sizeHigh * 4294967296 + sizeLow;
                  // Drift uses base units (1e9 for SOL)
                  size = rawSize / 1e9;
                  
                  // Sanity check: size should be reasonable (0.01 to 10000 SOL)
                  if (size < 0.01 || size > 10000) {
                    size = null;
                  }
                } catch (e) {
                  // Size parsing failed
                }
              }
              
              return {
                direction: potentialDirection === 0 ? 'long' : 'short',
                details: {
                  market: 'SOL-PERP',
                  market_index: marketIndex,
                  size: size,
                  raw_direction: potentialDirection,
                  discriminator: discHex,
                  parsed_at_offset: offset
                }
              };
            }
          }
        } catch (e) {
          continue;
        }
      }
      
      // ATTEMPT 3: If we can't parse details, at least return the discriminator
      return {
        direction: null,
        details: {
          discriminator: discHex,
          data_length: data.length,
          note: 'Could not parse instruction format - needs real Drift IDL'
        }
      };
      
    } catch (e) {
      console.error('Error parsing Drift instruction:', e.message);
    }
    
    return null;
  }

  // ========== Parse Jupiter Perp instruction ==========
  function parseJupiterPerpInstruction(instruction, accountKeys) {
    try {
      const data = Buffer.from(instruction.data, 'base64');
      
      if (data.length < 8) return null;

      // Log for debugging (first occurrence only)
      if (!parseJupiterPerpInstruction.logged) {
        console.log(`📊 Jupiter instruction data length: ${data.length} bytes`);
        console.log(`📊 First 32 bytes (hex): ${data.slice(0, Math.min(32, data.length)).toString('hex')}`);
        parseJupiterPerpInstruction.logged = true;
      }

      // Try to parse Jupiter perp format
      if (data.length >= 12) {
        const direction = data.readUInt8(8);
        
        if (direction === 0 || direction === 1) {
          return {
            direction: direction === 0 ? 'long' : 'short',
            details: {
              platform: 'jupiter',
              raw_data_length: data.length
            }
          };
        }
      }
      
      return {
        direction: null,
        details: {
          data_length: data.length,
          note: 'Could not parse Jupiter instruction format'
        }
      };
    } catch (e) {
      console.error('Error parsing Jupiter instruction:', e.message);
    }
    
    return null;
  }
  
  // Handle both array and single transaction format
  const transactions = Array.isArray(data) ? data : [data];
  
  transactions.forEach(block => {
    if (!block || !block.transactions) return;
    
    block.transactions.forEach(tx => {
      try {
        if (!tx.meta || tx.meta.err !== null) return;
        
        const accountKeys = tx.transaction?.message?.accountKeys || [];
        const instructions = tx.transaction?.message?.instructions || [];
        
        // ========== Detect perp platform interaction FIRST ==========
        const perpInfo = detectPerpInteraction(instructions, accountKeys);
        
        // Get the signer (the wallet making the transaction)
        const signerIndex = accountKeys.findIndex(acc => acc.signer === true);
        if (signerIndex === -1) return;
        
        const signer = accountKeys[signerIndex];
        const walletAddress = signer.pubkey;
        
        // Skip if it's a known exchange
        if (EXCLUDED_EXCHANGES.includes(walletAddress)) {
          skippedExchange++;
          return;
        }
        
        // Get balance information
        const preBalance = (tx.meta.preBalances?.[signerIndex] || 0) / 1e9;
        const postBalance = (tx.meta.postBalances?.[signerIndex] || 0) / 1e9;
        const fee = (tx.meta.fee || 0) / 1e9;
        
        // Calculate actual balance (post-transaction)
        const currentBalance = postBalance;
        
        // WHALE CHECK: Is this wallet large enough?
        if (currentBalance < MIN_WALLET_BALANCE && preBalance < MIN_WALLET_BALANCE) {
          skippedNotWhale++;
          return;
        }
        
        // Calculate SOL movement (excluding fees)
        const solChange = postBalance - preBalance - fee;
        const absSolChange = Math.abs(solChange);
        
        // Skip if movement is too small
        if (absSolChange < MIN_SOL_MOVEMENT) {
          skippedSmallMovement++;
          return;
        }
        
        // Determine direction
        let direction, action;
        if (solChange > 0) {
          direction = 'receiving';
          action = 'RECEIVED';
        } else {
          direction = 'sending';
          action = 'SENT';
        }
        
        // Classify whale size based on current balance
        let whaleType;
        if (currentBalance >= 100000) {
          whaleType = 'MEGA_WHALE';     // 100k+ SOL (~$15M+)
        } else if (currentBalance >= 50000) {
          whaleType = 'SUPER_WHALE';    // 50k-100k SOL (~$7.5M-15M)
        } else if (currentBalance >= 10000) {
          whaleType = 'WHALE';          // 10k-50k SOL (~$1.5M-7.5M)
        } else if (currentBalance >= 5000) {
          whaleType = 'LARGE_HOLDER';   // 5k-10k SOL (~$750k-1.5M)
        } else {
          whaleType = 'MODERATE_HOLDER'; // 1k-5k SOL (~$150k-750k)
        }
        
        // Classify movement significance
        let movementSignificance;
        if (absSolChange >= 5000) {
          movementSignificance = 'CRITICAL';  // 5000+ SOL movement
        } else if (absSolChange >= 1000) {
          movementSignificance = 'HIGH';      // 1000-5000 SOL
        } else if (absSolChange >= 500) {
          movementSignificance = 'MEDIUM';    // 500-1000 SOL
        } else {
          movementSignificance = 'LOW';       // 50-500 SOL
        }
        
        // Calculate percentage of balance moved
        const percentageMoved = preBalance > 0 
          ? parseFloat(((absSolChange / preBalance) * 100).toFixed(2))
          : 0;
        
        // ========== Add perp information AND raw data to whale movement ==========
        whaleMovements.push({
          signature: tx.transaction.signatures?.[0] || 'unknown',
          wallet_address: walletAddress,
          whale_type: whaleType,
          current_balance: parseFloat(currentBalance.toFixed(2)),
          sol_change: parseFloat(solChange.toFixed(4)),
          abs_change: parseFloat(absSolChange.toFixed(4)),
          percentage_moved: percentageMoved,
          direction: direction,
          action: action,
          movement_significance: movementSignificance,
          previous_balance: parseFloat(preBalance.toFixed(2)),
          fee_paid: parseFloat(fee.toFixed(6)),
          block_time: block.blockTime,
          timestamp: new Date(block.blockTime * 1000).toISOString(),
          received_at: functionReceivedISO,
          slot: block.parentSlot,
          
          // Perp position fields
          has_perp_position: perpInfo.has_perp_position,
          perp_platform: perpInfo.perp_platform,
          perp_direction: perpInfo.perp_direction,
          perp_size: perpInfo.perp_details?.size || null,
          perp_leverage: null, // Not easily determinable from transaction alone
          perp_entry_price: null, // Not easily determinable from transaction alone
          perp_debug_info: perpInfo.perp_details || null,  // Include debug info to help troubleshoot
          
          // 🔥 CRITICAL FIX: Add raw data to EACH whale movement
          raw_data_json: data  // Store complete transaction data for this movement
        });
        
        // Log significant whale movements
        if (movementSignificance === 'CRITICAL' || whaleType === 'MEGA_WHALE') {
          const perpInfo_str = perpInfo.has_perp_position 
            ? ` [${perpInfo.perp_platform?.toUpperCase()} ${perpInfo.perp_direction || 'UNKNOWN'}]`
            : '';
          console.log(`🐋 ${whaleType} ${action} ${absSolChange.toFixed(0)} SOL (${percentageMoved}% of balance)${perpInfo_str}`);
        }
        
      } catch (error) {
        console.error('Error processing whale transaction:', error.message);
      }
    });
  });
  
  // Return formatted results
  if (whaleMovements.length > 0 || skippedNotWhale > 0) {
    // Calculate summary statistics
    const totalVolume = whaleMovements.reduce((sum, m) => sum + m.abs_change, 0);
    const receiving = whaleMovements.filter(m => m.direction === 'receiving');
    const sending = whaleMovements.filter(m => m.direction === 'sending');
    
    const netFlow = receiving.reduce((sum, m) => sum + m.abs_change, 0) - 
                    sending.reduce((sum, m) => sum + m.abs_change, 0);
    
    // Breakdown by whale type
    const whaleTypeBreakdown = {
      mega_whale: whaleMovements.filter(m => m.whale_type === 'MEGA_WHALE').length,
      super_whale: whaleMovements.filter(m => m.whale_type === 'SUPER_WHALE').length,
      whale: whaleMovements.filter(m => m.whale_type === 'WHALE').length,
      large_holder: whaleMovements.filter(m => m.whale_type === 'LARGE_HOLDER').length,
      moderate_holder: whaleMovements.filter(m => m.whale_type === 'MODERATE_HOLDER').length
    };
    
    // Breakdown by movement significance
    const significanceBreakdown = {
      critical: whaleMovements.filter(m => m.movement_significance === 'CRITICAL').length,
      high: whaleMovements.filter(m => m.movement_significance === 'HIGH').length,
      medium: whaleMovements.filter(m => m.movement_significance === 'MEDIUM').length,
      low: whaleMovements.filter(m => m.movement_significance === 'LOW').length
    };
    
    // Perp statistics
    const perpBreakdown = {
      with_perp: whaleMovements.filter(m => m.has_perp_position).length,
      without_perp: whaleMovements.filter(m => !m.has_perp_position).length,
      drift: whaleMovements.filter(m => m.perp_platform === 'drift').length,
      jupiter: whaleMovements.filter(m => m.perp_platform === 'jupiter').length,
      mango: whaleMovements.filter(m => m.perp_platform === 'mango').length,
      zeta: whaleMovements.filter(m => m.perp_platform === 'zeta').length,
      longs: whaleMovements.filter(m => m.perp_direction === 'long').length,
      shorts: whaleMovements.filter(m => m.perp_direction === 'short').length
    };
    
    const result = {
      whaleMovements: whaleMovements,
      summary: {
        totalMovements: whaleMovements.length,
        totalVolume: parseFloat(totalVolume.toFixed(2)),
        netFlow: parseFloat(netFlow.toFixed(2)),
        receiving: receiving.length,
        sending: sending.length,
        whaleTypeBreakdown: whaleTypeBreakdown,
        significanceBreakdown: significanceBreakdown,
        perpBreakdown: perpBreakdown
      },
      processingTimestamp: functionReceivedISO,
      blockHeight: transactions[0]?.parentSlot || null
    };
    
    // Add skip counts if any
    if (skippedNotWhale > 0) {
      result.skippedNotWhale = skippedNotWhale;
    }
    if (skippedSmallMovement > 0) {
      result.skippedSmallMovement = skippedSmallMovement;
    }
    if (skippedExchange > 0) {
      result.skippedExchange = skippedExchange;
    }
    
    return result;
  }
  
  return null;
}

