FROM alpine:3.20

RUN apk add --no-cache openssh-server procps coreutils findutils \
    && mkdir -p /var/run/sshd \
    && ssh-keygen -A \
    && echo 'root:rootpwd' | chpasswd \
    && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && echo 'UsePAM no' >> /etc/ssh/sshd_config \
    && echo 'PermitEmptyPasswords no' >> /etc/ssh/sshd_config

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D", "-e"]
