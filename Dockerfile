FROM mysql:8.4.3

# ログディレクトリを作成
RUN mkdir -p /var/log/mysql && \
    chown mysql:mysql /var/log/mysql

# カスタム設定ファイルをコピー
COPY config/my.cnf /etc/mysql/conf.d/custom.cnf

# ヘルスチェックを追加
HEALTHCHECK --interval=30s --timeout=3s --start-period=30s --retries=3 \
  CMD mysqladmin ping -h localhost -u root -p$MYSQL_ROOT_PASSWORD || exit 1

EXPOSE 3306
