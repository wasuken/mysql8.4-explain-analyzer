.PHONY: all benchmark clean

all: benchmark

benchmark:
	@echo "🚀 統合ベンチマーク実行"
	sql/data/.venv/bin/python sql/data/benchmark.py

clean:
	@echo "🧹 全データ削除"
	sql/data/.venv/bin/python sql/data/clean_data_generator.py

setup:
	@echo "🔧 Docker環境起動"
	docker compose up -d
	@sleep 10
	@echo "📊 データ生成"
	sql/data/.venv/bin/python sql/data/clean_data_generator.py
	@echo "✅ セットアップ完了"
