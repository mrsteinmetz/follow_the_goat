# Filter Analysis Page - Settings Investigation

## Current Status

### ✅ Settings That ARE Present:
The current page (`/root/follow_the_goat/000website/pages/features/filter-analysis/index.php`) **already has** the Auto Filter Settings section with:

1. **Good Trade Threshold (%)** - Input field with step 0.1, min 0.1, max 5.0
2. **Analysis Hours** - Input field with min 1, max 168
3. **Minimum Filters in Combo** - Input field with min 1, max 10
4. **Save Settings** button
5. **Reset to Defaults** button

### How It Works:
- The settings panel is **collapsible** (Bootstrap collapse)
- Click on the "Auto Filter Settings" header to expand/collapse
- Current values are shown in a badge: `Good: 0.3% | Hours: 24 | Min Filters: 1`
- Arrow icon rotates when expanded/collapsed

### UI Location:
```php
<!-- Lines 352-407 in current index.php -->
<div class="card custom-card mb-3">
    <div class="card-header d-flex justify-content-between align-items-center" 
         style="cursor: pointer;" 
         data-bs-toggle="collapse" 
         data-bs-target="#settingsPanel">
        <h6 class="mb-0"><i class="ri-settings-3-line me-1"></i>Auto Filter Settings</h6>
        ...
    </div>
    <div class="collapse" id="settingsPanel">
        <div class="card-body">
            <form id="settingsForm">
                <!-- Three input fields for settings -->
            </form>
        </div>
    </div>
</div>
```

## Question for User

**What specific "advanced settings" feature are you referring to?**

### Possibilities:

1. **Toggle/Checkbox Missing?**
   - Was there an "Enable Advanced Settings" checkbox that showed additional fields?
   - Was there a "Show Advanced Options" button?

2. **Additional Settings Fields?**
   - Were there more than 3 settings fields in the old version?
   - Any dropdown menus, radio buttons, or other controls?

3. **UI Interaction Issue?**
   - Is the collapse/expand not working?
   - Are the input fields disabled or read-only?

### Old Code Findings:
- The old code (`000old_code/solana_node/v2/filter-analizes/index.php`) appears to have a similar structure
- No obvious "advanced settings" toggle or additional fields found
- Both versions have the same 3 core settings

## Next Steps

Please clarify:
1. **What was the exact name/label** of the "advanced settings" option?
2. **What additional fields/options** did it reveal when enabled?
3. **Where on the page** was this control located?
4. Or take a **screenshot** of the old version showing the advanced settings

This will help me restore the exact feature you're missing!

---

**Current File:** `/root/follow_the_goat/000website/pages/features/filter-analysis/index.php`
**Lines:** 352-407 (Auto Filter Settings section)
**Status:** ⚠️ Needs clarification on what "advanced settings" means
