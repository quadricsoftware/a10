## This is a multi stage build.  First we make the various compiled binaries in temp images, then we copy them into the main image below
# Let's start with the go binaries...
FROM golang AS gobuilder
WORKDIR /app
COPY src/go/go.mod src/go/go.sum ./
RUN go mod download
COPY ./src/go .
RUN GOOS=linux go build -o /app/bin/cloudtool -ldflags "-linkmode 'external' -extldflags '-static'" ./binaries/cloudtool
RUN GOOS=linux go build -o /app/bin/munger -ldflags "-linkmode 'external' -extldflags '-static'" ./binaries/munger
RUN GOOS=linux go build -o /app/bin/devmounter -ldflags "-linkmode 'external' -extldflags '-static'" ./binaries/devmounter
RUN GOOS=linux go build -o /app/bin/agent -ldflags "-linkmode 'external' -extldflags '-static'" ./binaries/agent-linux
RUN GOOS=windows GOARCH=amd64 go build -o /app/bin/munger.exe ./binaries/munger


## Now we move onto the main image
FROM alpine:3.20.1
LABEL Maintainer="Quadric Software <code@quadricsoftware.com>"
LABEL Description="Quadric Next Gen Backup"

RUN apk add --no-cache \
  curl \
  nginx \
  nginx-mod-http-headers-more \
  php83 \
  php83-curl \
  php83-fpm \
  php83-openssl \
  php83-pdo_sqlite \
  php83-session \
  php83-pcntl \
  php83-posix \
  qemu-img \
  findmnt \
  ncurses \
  fstrim \
  sshpass \
  lsblk \
  python3 \
  py3-pip \
  nbd \
  nbd-client \
  btrfs-progs \
  zstd \
  sudo \
  msmtp \
  mailx \
  dropbear \
    php83-pear \
    php83-dev \
    gcc \
    g++ \
    make \
    zstd-dev

# copy the compiled go binaries into the main image
COPY --from=gobuilder /app/bin/* /backup/app/bin/

######### Install zstd extension using PECL
RUN pecl install zstd 
# Clean up build dependencies
RUN apk del php83-pear php83-dev gcc g++ make zstd-dev && apk cache clean
############

RUN addgroup -g 1000 admin && adduser -u 1000 -G admin -s /bin/ash -D admin && pip install XenAPI --break-system-packages && pip install requests --break-system-packages

WORKDIR /backup
RUN mkdir -p  /backup/logs

# copy all of the app scripts, etc
COPY --chown=admin app /backup/app

# Configure nginx - http
COPY configs/nginx.conf.main /etc/nginx/nginx.conf
COPY configs/nginx.conf /backup/configs/nginx.conf
COPY certs/* /backup/certs/
COPY configs/dropbear.conf /etc/conf.d/dropbear

# Configure PHP-FPM
COPY configs/php-fpm.conf /etc/php83/php-fpm.d/www.conf
COPY configs/php.ini /etc/php83/php.ini

EXPOSE 8080 8081 80 443 22

# Set the startup script
COPY start.sh /start.sh
# generate rsa keys for ssh
RUN chmod +x /start.sh && dropbearkey -t rsa -s 4096 -f /etc/dropbear/dropbear_rsa_host_key

# Set the entrypoint to our startup script
ENTRYPOINT ["/start.sh"]
