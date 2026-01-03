#!/bin/bash
# =============================================================================
# MySQL Installation and Setup for Ubuntu/WSL
# =============================================================================
# This script installs MySQL on Ubuntu and creates the archive database.
# Run this script from within WSL Ubuntu.
#
# Usage:
#   chmod +x scripts/setup_mysql_ubuntu.sh
#   ./scripts/setup_mysql_ubuntu.sh
# =============================================================================

set -e

echo "============================================================"
echo "MySQL Setup for Follow The Goat - Archive Database"
echo "============================================================"

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then
    echo "Please run with sudo: sudo ./scripts/setup_mysql_ubuntu.sh"
    exit 1
fi

# Update package list
echo ""
echo "[1/6] Updating package list..."
apt update

# Install MySQL server
echo ""
echo "[2/6] Installing MySQL server..."
apt install mysql-server -y

# Start MySQL service
echo ""
echo "[3/6] Starting MySQL service..."
systemctl start mysql
systemctl enable mysql

# Check if MySQL is running
if systemctl is-active --quiet mysql; then
    echo "MySQL is running!"
else
    echo "ERROR: MySQL failed to start"
    exit 1
fi

# Create database and user
echo ""
echo "[4/6] Creating archive database and user..."

# Generate a random password if not provided
DB_PASSWORD="${FTG_DB_PASSWORD:-$(openssl rand -base64 16)}"

mysql -e "CREATE DATABASE IF NOT EXISTS follow_the_goat_archive CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
mysql -e "CREATE USER IF NOT EXISTS 'ftg_user'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';"
mysql -e "GRANT ALL PRIVILEGES ON follow_the_goat_archive.* TO 'ftg_user'@'localhost';"
mysql -e "FLUSH PRIVILEGES;"

echo ""
echo "[5/6] Creating archive tables..."

# Run the archive schema
mysql follow_the_goat_archive < "$(dirname "$0")/mysql_archive_schema.sql"

echo ""
echo "[6/6] Verifying setup..."
mysql -e "SHOW DATABASES;" | grep follow_the_goat_archive
mysql -e "USE follow_the_goat_archive; SHOW TABLES;"

echo ""
echo "============================================================"
echo "MySQL Setup Complete!"
echo "============================================================"
echo ""
echo "Database: follow_the_goat_archive"
echo "User: ftg_user"
echo "Password: ${DB_PASSWORD}"
echo ""
echo "Add these to your .env file:"
echo ""
echo "DB_HOST=127.0.0.1"
echo "DB_USER=ftg_user"
echo "DB_PASSWORD=${DB_PASSWORD}"
echo "DB_DATABASE=follow_the_goat_archive"
echo "DB_PORT=3306"
echo ""
echo "============================================================"

