    cd boot
    cp cmdline.txt ssh
    vi cmdline.txt
       ip=192.168.137.172

    cd /etc/network
    vi interfaces
       auto eth0:0
       iface eth0:0 inet static
       address 172.30.0.137
       netmask 255.255.0.0
       gateway 172.30.0.234
       dns-nameservers 223.5.5.5
    ifup eth0:0

    cd /usr/local/src
    apt-get install hostapd dnsmasq
    wget https://github.com/oblique/create_ap/archive/v0.4.6.tar.gz
    tar -zxvf v0.4.6.tar.gz
    cd create_ap-0.4.6
    make install
    create_ap wlan0 eth0 MyAccessPoint MyPassPhrase

    cd /usr/src
    apt-get install git
    mkdir raswitch
    cd raswitch
    git init
    git config --global user.email wang.haitao@msn.com
    git add README.md
    git commit -m 'new file'
    git remote add origin https://github.com/raswitch/raswitch.git 
    git push -u origin master

    cd /usr/local/src
    apt-get install autoconf libtool-bin libjpeg-dev libsqlite3-dev libcurl4-openssl-dev libpcre3-dev libspeex-dev libspeexdsp-dev libldns-dev libedit-dev liblua5.2-dev libopus-dev libsndfile-dev
    wget http://files.freeswitch.org/releases/freeswitch/freeswitch-1.6.13.tar.gz
    tar -zxvf freeswitch-1.6.13.tar.gz
    cd freeswitch-1.6.13
    ./rebootstrap.sh
    ./configure
    make
    make install
    make sounds-install
    make moh-install

