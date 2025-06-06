# app.py
from datetime import datetime

import config
from flask import Flask, jsonify, request
from flask_cors import CORS
from models import HoldingStock, db

app = Flask(__name__)

# 配置 Flask 和数据库
for key, value in config.FLASK_CONFIG.items():
    app.config[key] = value

app.config["SQLALCHEMY_DATABASE_URI"] = (
    f"mysql+pymysql://{config.DB_CONFIG['user']}:{config.DB_CONFIG['password']}@"
    f"{config.DB_CONFIG['host']}/{config.DB_CONFIG['database']}?charset={config.DB_CONFIG['charset']}"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# 初始化扩展
db.init_app(app)
CORS(app)


@app.route("/api/stocks", methods=["GET"])
def get_stocks():
    stocks = HoldingStock.query.all()
    return jsonify([stock.to_dict() for stock in stocks])


@app.route("/api/stocks/<int:id>", methods=["GET"])
def get_stock(id):
    stock = HoldingStock.query.get_or_404(id)
    return jsonify(stock.to_dict())


@app.route("/api/stocks", methods=["POST"])
def add_stock():
    data = request.get_json()

    new_stock = HoldingStock(
        ts_code=data["ts_code"],
        name=data["name"],
        buy_price=data["buy_price"],
        buy_date=datetime.strptime(data["buy_date"], "%Y-%m-%d").date(),
        status=data.get("status", "holding"),
        sell_price=data.get("sell_price"),
        sell_date=datetime.strptime(data["sell_date"], "%Y-%m-%d").date() if data.get("sell_date") else None,
        reason=data.get("reason"),
    )

    db.session.add(new_stock)
    db.session.commit()

    return jsonify(new_stock.to_dict()), 201


@app.route("/api/stocks/<int:id>", methods=["PUT"])
def update_stock(id):
    stock = HoldingStock.query.get_or_404(id)
    data = request.get_json()

    stock.ts_code = data.get("ts_code", stock.ts_code)
    stock.name = data.get("name", stock.name)
    stock.buy_price = data.get("buy_price", stock.buy_price)
    stock.buy_date = (
        datetime.strptime(data.get("buy_date"), "%Y-%m-%d").date() if data.get("buy_date") else stock.buy_date
    )
    stock.status = data.get("status", stock.status)
    stock.sell_price = data.get("sell_price", stock.sell_price)
    stock.sell_date = (
        datetime.strptime(data.get("sell_date"), "%Y-%m-%d").date() if data.get("sell_date") else stock.sell_date
    )
    stock.reason = data.get("reason", stock.reason)

    db.session.commit()

    return jsonify(stock.to_dict())


@app.route("/api/stocks/<int:id>", methods=["DELETE"])
def delete_stock(id):
    stock = HoldingStock.query.get_or_404(id)
    db.session.delete(stock)
    db.session.commit()

    return jsonify({"message": "Stock deleted successfully"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
