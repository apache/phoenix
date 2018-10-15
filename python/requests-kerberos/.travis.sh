#!/bin/bash

set -e

IP_ADDRESS=$(hostname -I)
HOSTNAME=$(cat /etc/hostname)
PY_MAJOR=${PYENV:0:1}

export KERBEROS_HOSTNAME=$HOSTNAME.$KERBEROS_REALM
export DEBIAN_FRONTEND=noninteractive

echo "Configure the hosts file for Kerberos to work in a container"
cp /etc/hosts ~/hosts.new
sed -i "/.*$HOSTNAME/c\\$IP_ADDRESS\t$KERBEROS_HOSTNAME" ~/hosts.new
cp -f ~/hosts.new /etc/hosts

echo "Setting up Kerberos config file at /etc/krb5.conf"
cat > /etc/krb5.conf << EOL
[libdefaults]
    default_realm = ${KERBEROS_REALM^^}
    dns_lookup_realm = false
    dns_lookup_kdc = false

[realms]
    ${KERBEROS_REALM^^} = {
        kdc = $KERBEROS_HOSTNAME
        admin_server = $KERBEROS_HOSTNAME
    }

[domain_realm]
    .$KERBEROS_REALM = ${KERBEROS_REALM^^}

[logging]
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmin.log
    default = FILE:/var/log/krb5lib.log
EOL

echo "Setting up kerberos ACL configuration at /etc/krb5kdc/kadm5.acl"
mkdir /etc/krb5kdc
echo -e "*/*@${KERBEROS_REALM^^}\t*" > /etc/krb5kdc/kadm5.acl

echo "Installing all the packages required in this test"
apt-get update
apt-get \
    -y \
    -qq \
    install \
    krb5-{user,kdc,admin-server,multidev} \
    libkrb5-dev \
    wget \
    curl \
    apache2 \
    libapache2-mod-auth-gssapi \
    python-dev \
    libffi-dev \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev

echo "Creating KDC database"
# krb5_newrealm returns non-0 return code as it is running in a container, ignore it for this command only
set +e
printf "$KERBEROS_PASSWORD\n$KERBEROS_PASSWORD" | krb5_newrealm
set -e

echo "Creating principals for tests"
kadmin.local -q "addprinc -pw $KERBEROS_PASSWORD $KERBEROS_USERNAME"

echo "Adding HTTP principal for Kerberos and create keytab"
kadmin.local -q "addprinc -randkey HTTP/$KERBEROS_HOSTNAME"
kadmin.local -q "ktadd -k /etc/krb5.keytab HTTP/$KERBEROS_HOSTNAME"
chmod 777 /etc/krb5.keytab

echo "Restarting Kerberos KDS service"
service krb5-kdc restart

echo "Add ServerName to Apache config"
grep -q -F "ServerName $KERBEROS_HOSTNAME" /etc/apache2/apache2.conf || echo "ServerName $KERBEROS_HOSTNAME" >> /etc/apache2/apache2.conf

echo "Deleting default virtual host file"
rm /etc/apache2/sites-enabled/000-default.conf
rm /etc/apache2/sites-available/000-default.conf
rm /etc/apache2/sites-available/default-ssl.conf

echo "Create website directory structure and pages"
mkdir -p /var/www/example.com/public_html
chmod -R 755 /var/www
echo "<html><head><title>Title</title></head><body>body mesage</body></html>" > /var/www/example.com/public_html/index.html

echo "Create self signed certificate for HTTPS endpoint"
mkdir /etc/apache2/ssl
openssl req \
    -x509 \
    -nodes \
    -days 365 \
    -newkey rsa:2048 \
    -keyout /etc/apache2/ssl/https.key \
    -out /etc/apache2/ssl/https.crt \
    -subj "/CN=$KERBEROS_HOSTNAME/o=Testing LTS./C=US"

echo "Create virtual host files"
cat > /etc/apache2/sites-available/example.com.conf << EOL
<VirtualHost *:80>
    ServerName $KERBEROS_HOSTNAME
    ServerAlias $KERBEROS_HOSTNAME
    DocumentRoot /var/www/example.com/public_html
    ErrorLog ${APACHE_LOG_DIR}/error.log
    CustomLog ${APACHE_LOG_DIR}/access.log combined
    <Directory "/var/www/example.com/public_html">
        AuthType GSSAPI
        AuthName "GSSAPI Single Sign On Login"
        Require user $KERBEROS_USERNAME@${KERBEROS_REALM^^}
        GssapiCredStore keytab:/etc/krb5.keytab
    </Directory>
</VirtualHost>
<VirtualHost *:443>
    ServerName $KERBEROS_HOSTNAME
    ServerAlias $KERBEROS_HOSTNAME
    DocumentRoot /var/www/example.com/public_html
    ErrorLog ${APACHE_LOG_DIR}/error.log
    CustomLog ${APACHE_LOG_DIR}/access.log combined
    SSLEngine on
    SSLCertificateFile /etc/apache2/ssl/https.crt
    SSLCertificateKeyFile /etc/apache2/ssl/https.key
    <Directory "/var/www/example.com/public_html">
        AuthType GSSAPI
        AuthName "GSSAPI Single Sign On Login"
        Require user $KERBEROS_USERNAME@${KERBEROS_REALM^^}
        GssapiCredStore keytab:/etc/krb5.keytab
    </Directory>
</VirtualHost>
EOL

echo "Enabling virtual host site"
a2enmod ssl
a2ensite example.com.conf
service apache2 restart

echo "Getting ticket for Kerberos user"
echo -n "$KERBEROS_PASSWORD" | kinit "$KERBEROS_USERNAME@${KERBEROS_REALM^^}"

echo "Try out the HTTP connection with curl"
CURL_OUTPUT=$(curl --negotiate -u : "http://$KERBEROS_HOSTNAME")

if [ "$CURL_OUTPUT" != "<html><head><title>Title</title></head><body>body mesage</body></html>" ]; then
    echo -e "ERROR: Did not get success message, cannot continue with actual tests:\nActual Output:\n$CURL_OUTPUT"
    exit 1
else
    echo -e "SUCCESS: Apache site built and set for Kerberos auth\nActual Output:\n$CURL_OUTPUT"
fi

echo "Try out the HTTPS connection with curl"
CURL_OUTPUT=$(curl --negotiate -u : "https://$KERBEROS_HOSTNAME" --insecure)

if [ "$CURL_OUTPUT" != "<html><head><title>Title</title></head><body>body mesage</body></html>" ]; then
    echo -e "ERROR: Did not get success message, cannot continue with actual tests:\nActual Output:\n$CURL_OUTPUT"
    exit 1
else
    echo -e "SUCCESS: Apache site built and set for Kerberos auth\nActual Output:\n$CURL_OUTPUT"
fi

if [ "$IMAGE" == "ubuntu:16.04" ]; then
    echo "Downloading Python $PYENV"
    wget -q "https://www.python.org/ftp/python/$PYENV/Python-$PYENV.tgz"
    tar xzf "Python-$PYENV.tgz"
    cd "Python-$PYENV"

    echo "Configuring Python install"
    ./configure &> /dev/null

    echo "Running make install on Python"
    make install &> /dev/null
    cd ..
    rm -rf "Python-$PYENV"
    rm "Python-$PYENV.tgz"
fi

echo "Installing Pip"
wget -q https://bootstrap.pypa.io/get-pip.py
python$PY_MAJOR get-pip.py
rm get-pip.py

echo "Updating pip and installing library"
pip$PY_MAJOR install -U pip setuptools
pip$PY_MAJOR install .
pip$PY_MAJOR install -r requirements-test.txt

echo "Outputting build info before tests"
echo "Python Version: $(python$PY_MAJOR --version 2>&1)"
echo "Pip Version: $(pip$PY_MAJOR --version)"
echo "Pip packages: $(pip$PY_MAJOR list)"

echo "Running Python tests"
export KERBEROS_PRINCIPAL="$KERBEROS_USERNAME@${KERBEROS_REALM^^}"
export KERBEROS_URL="http://$KERBEROS_HOSTNAME"
python$PY_MAJOR -m pytest -v --cov=requests_kerberos\

echo "Running Python test over HTTPS for basic CBT test"
export KERBEROS_URL="https://$KERBEROS_HOSTNAME"
python$PY_MAJOR -m pytest -v --cov=requests_kerberos\
