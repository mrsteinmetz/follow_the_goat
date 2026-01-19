<?php
/**
 * Temporary Authentication System
 * 
 * Simple password-based authentication with cookie persistence.
 * Password: 79132026
 * Cookie expires in 365 days.
 */

// Define the temporary password
define('TEMP_PASSWORD', '79132026');
define('AUTH_COOKIE_NAME', 'ftg_temp_auth');
define('AUTH_COOKIE_EXPIRY', 365 * 24 * 60 * 60); // 365 days in seconds

/**
 * Check if user is authenticated
 * @return bool True if authenticated, false otherwise
 */
function isAuthenticated() {
    // Check if cookie exists and is valid
    if (isset($_COOKIE[AUTH_COOKIE_NAME])) {
        // Verify the cookie value matches our password (simple check)
        // In production, you'd want to hash this, but for temp password this is fine
        return $_COOKIE[AUTH_COOKIE_NAME] === hash('sha256', TEMP_PASSWORD);
    }
    return false;
}

/**
 * Set authentication cookie
 */
function setAuthCookie() {
    // Set cookie with 365 day expiry
    $cookieValue = hash('sha256', TEMP_PASSWORD);
    setcookie(
        AUTH_COOKIE_NAME,
        $cookieValue,
        time() + AUTH_COOKIE_EXPIRY,
        '/', // Available site-wide
        '', // Domain (empty = current domain)
        false, // Secure (set to true if using HTTPS)
        true // HttpOnly (prevent JavaScript access)
    );
}

/**
 * Clear authentication cookie
 */
function clearAuthCookie() {
    setcookie(
        AUTH_COOKIE_NAME,
        '',
        time() - 3600, // Expire immediately
        '/',
        '',
        false,
        true
    );
}

/**
 * Handle login form submission
 * @return bool True if login successful, false otherwise
 */
function handleLogin() {
    if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['password'])) {
        $enteredPassword = $_POST['password'];
        
        if ($enteredPassword === TEMP_PASSWORD) {
            setAuthCookie();
            return true;
        }
    }
    return false;
}

/**
 * Check if current page is the login page
 * @return bool True if on login page
 */
function isLoginPage() {
    $script_name = $_SERVER['SCRIPT_NAME'] ?? '';
    $request_uri = $_SERVER['REQUEST_URI'] ?? '';
    
    // Check if script is in temp_login directory
    if (strpos($script_name, '/temp_login/') !== false) {
        return true;
    }
    
    // Check if request URI points to temp_login
    if (strpos($request_uri, '/temp_login') !== false) {
        return true;
    }
    
    return false;
}

/**
 * Require authentication - redirect to login if not authenticated
 * Call this at the top of protected pages
 */
function requireAuth() {
    // Don't require auth on login page itself
    if (isLoginPage()) {
        return;
    }
    
    // Handle login form submission (in case form is submitted elsewhere)
    if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['password'])) {
        if (handleLogin()) {
            // Redirect to remove POST data from URL
            $redirect = $_GET['redirect'] ?? $_SERVER['PHP_SELF'];
            header('Location: ' . $redirect);
            exit;
        }
    }
    
    // Check if already authenticated
    if (!isAuthenticated()) {
        // Get current page URL to redirect back after login
        $currentUrl = $_SERVER['REQUEST_URI'] ?? $_SERVER['PHP_SELF'];
        $loginUrl = '/temp_login/?redirect=' . urlencode($currentUrl);
        
        // Redirect to login page
        header('Location: ' . $loginUrl);
        exit;
    }
}
