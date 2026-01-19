<?php
/**
 * Temporary Login Page
 * 
 * Simple password-based authentication.
 * Password: 79132026
 */

// Set timezone
date_default_timezone_set('UTC');

// Include auth functions
require_once __DIR__ . '/../includes/auth.php';

// Handle login
$error_message = '';
$login_success = false;

if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['password'])) {
    if (handleLogin()) {
        $login_success = true;
        // Get redirect URL or default to home
        $redirect = $_GET['redirect'] ?? '/';
        // Redirect after a brief moment
        header('Refresh: 1; url=' . $redirect);
    } else {
        $error_message = 'Invalid password. Please try again.';
    }
}

// If already authenticated, redirect
if (isAuthenticated() && !isset($_POST['password'])) {
    $redirect = $_GET['redirect'] ?? '/';
    header('Location: ' . $redirect);
    exit;
}

?>
<!DOCTYPE html>
<html lang="en" dir="ltr" data-theme-mode="dark">
<head>
    <meta charset="UTF-8">
    <meta name='viewport' content='width=device-width, initial-scale=1.0'>
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <title>Login - Follow The Goat</title>
    
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #0f1117 0%, #1a1d29 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
        }
        
        .login-container {
            background: rgba(30, 30, 46, 0.95);
            border-radius: 16px;
            padding: 40px;
            width: 100%;
            max-width: 400px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
            border: 1px solid rgba(16, 185, 129, 0.2);
        }
        
        .login-header {
            text-align: center;
            margin-bottom: 30px;
        }
        
        .login-header h1 {
            color: #10b981;
            font-size: 28px;
            font-weight: 600;
            margin-bottom: 8px;
        }
        
        .login-header p {
            color: #9ca3af;
            font-size: 14px;
        }
        
        .form-group {
            margin-bottom: 20px;
        }
        
        .form-group label {
            display: block;
            color: #d1d5db;
            font-size: 14px;
            font-weight: 500;
            margin-bottom: 8px;
        }
        
        .form-group input {
            width: 100%;
            padding: 12px 16px;
            background: rgba(17, 24, 39, 0.8);
            border: 1px solid rgba(75, 85, 99, 0.5);
            border-radius: 8px;
            color: #ffffff;
            font-size: 16px;
            transition: all 0.2s;
        }
        
        .form-group input:focus {
            outline: none;
            border-color: #10b981;
            box-shadow: 0 0 0 3px rgba(16, 185, 129, 0.1);
        }
        
        .btn-login {
            width: 100%;
            padding: 12px 24px;
            background: #10b981;
            color: #ffffff;
            border: none;
            border-radius: 8px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
            margin-top: 10px;
        }
        
        .btn-login:hover {
            background: #059669;
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);
        }
        
        .btn-login:active {
            transform: translateY(0);
        }
        
        .error-message {
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid rgba(239, 68, 68, 0.3);
            color: #fca5a5;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 20px;
            font-size: 14px;
        }
        
        .success-message {
            background: rgba(16, 185, 129, 0.1);
            border: 1px solid rgba(16, 185, 129, 0.3);
            color: #6ee7b7;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 20px;
            font-size: 14px;
            text-align: center;
        }
        
        .footer-text {
            text-align: center;
            margin-top: 20px;
            color: #6b7280;
            font-size: 12px;
        }
    </style>
</head>
<body>
    <div class="login-container">
        <div class="login-header">
            <h1>🔐 Login</h1>
            <p>Enter your password to continue</p>
        </div>
        
        <?php if ($error_message): ?>
            <div class="error-message">
                <?php echo htmlspecialchars($error_message); ?>
            </div>
        <?php endif; ?>
        
        <?php if ($login_success): ?>
            <div class="success-message">
                ✓ Login successful! Redirecting...
            </div>
        <?php else: ?>
            <form method="POST" action="">
                <div class="form-group">
                    <label for="password">Password</label>
                    <input 
                        type="password" 
                        id="password" 
                        name="password" 
                        placeholder="Enter password"
                        required
                        autofocus
                        autocomplete="current-password"
                    >
                </div>
                
                <button type="submit" class="btn-login">
                    Login
                </button>
            </form>
        <?php endif; ?>
        
        <div class="footer-text">
            Follow The Goat Dashboard
        </div>
    </div>
</body>
</html>
