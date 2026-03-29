FROM emqx/emqx-enterprise:6.1.1

COPY docker-entrypoint.sh /usr/local/bin/lucid-emqx-entrypoint.sh
RUN chmod +x /usr/local/bin/lucid-emqx-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/lucid-emqx-entrypoint.sh"]
