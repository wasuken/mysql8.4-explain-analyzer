# バッファプールの調査メモ

## バッファプール統計情報の取得

```sql
SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_read%';
```

ベンチマーク

+---------------------------------------+---------+
| Variable_name                         | Value   |
+---------------------------------------+---------+
| Innodb_buffer_pool_read_ahead_rnd     | 0       |
| Innodb_buffer_pool_read_ahead         | 4928    |
| Innodb_buffer_pool_read_ahead_evicted | 0       |
| Innodb_buffer_pool_read_requests      | 6392131 |
| Innodb_buffer_pool_reads              | 7062    |
+---------------------------------------+---------+
5 rows in set (0.03 sec)

各項目の意味
Innodb_buffer_pool_read_requests (6,392,131)
MySQLがBuffer Poolからデータを読み取ろうとした総要求回数だ。

アプリケーションが「データくれ」って言った回数
高い = システムが活発に動いてる証拠

Innodb_buffer_pool_reads (7,062)
Buffer Poolにデータがなくて、実際にディスクから読み取った回数だ。

キャッシュミス = ディスクI/O発生
低い方が良い（メモリから読む方が圧倒的に速い）

Innodb_buffer_pool_read_ahead (4,928)
MySQLが「次もこのデータ使うだろうな」って先読みした回数だ。

予測してディスクからメモリに先にロード
効率的なI/Oパターンの証拠

Innodb_buffer_pool_read_ahead_evicted (0)
先読みしたけど使われずに追い出されたページ数だ。

0 = 予測が100%的中
お前のシステム、予測精度が完璧

Innodb_buffer_pool_read_ahead_rnd (0)
ランダムリードアヘッドの回数だ。

0 = 順次アクセスパターンが主体
データアクセスが効率的な証拠

パフォーマンス評価
Hit Rate: 99.89%
(6,392,131 - 7,062) ÷ 6,392,131 × 100 = 99.89%
これはプロレベルの数字だ。
Read Ahead効率: 100%
先読みした4,928ページが全部使われてる。無駄ゼロ。
