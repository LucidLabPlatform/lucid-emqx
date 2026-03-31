FROM emqx/emqx-enterprise:6.1.1

COPY --chmod=755 docker-entrypoint.sh /usr/local/bin/lucid-emqx-entrypoint.sh
COPY auth-built-in-db-bootstrap.csv /opt/emqx/etc/auth-built-in-db-bootstrap.csv

ENTRYPOINT ["/usr/local/bin/lucid-emqx-entrypoint.sh"]
CMD ["/opt/emqx/bin/emqx", "foreground"]
