from flask_sqlalchemy import SQLAlchemy

# Initialize SQLAlchemy
db = SQLAlchemy()


class HoldingStock(db.Model):
    __tablename__ = "holding_stock"

    id = db.Column(db.Integer, primary_key=True)
    ts_code = db.Column(db.String(10), nullable=False)
    name = db.Column(db.String(50))
    buy_price = db.Column(db.Float, nullable=False)
    buy_date = db.Column(db.Date, nullable=False)
    status = db.Column(db.Enum("holding", "sold"), default="holding")
    sell_price = db.Column(db.Float)
    sell_date = db.Column(db.Date)
    reason = db.Column(db.String(20))

    def __repr__(self):
        return f"<HoldingStock {self.id}>"

    def to_dict(self):
        return {
            "id": self.id,
            "ts_code": self.ts_code,
            "name": self.name,
            "buy_price": self.buy_price,
            "buy_date": self.buy_date.strftime("%Y-%m-%d") if self.buy_date else None,
            "status": self.status,
            "sell_price": self.sell_price,
            "sell_date": self.sell_date.strftime("%Y-%m-%d") if self.sell_date else None,
            "reason": self.reason,
        }
