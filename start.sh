#!/bin/sh

if [ -z "$ADMIN_PASSWORD" ]; then
    ADMIN_PASSWORD="admin"
fi
echo "admin:$ADMIN_PASSWORD" | chpasswd
echo "Set admin password"

dropbear

echo "Welcome to the A10!\n" > /etc/motd

mkdir -p /run/php
chown admin:admin /run/php

mkdir -p /mnt/raw
mkdir -p /mnt/storage
chown -R admin:admin /mnt
echo "admin ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers
chown -R admin:admin /backup

# Starting PHP
php-fpm83 &

nginx -g 'daemon off;'
