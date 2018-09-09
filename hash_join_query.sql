select /*+ USE_PERSISTENT_CACHE */ * from stock_symbol ss join stock_price sp on sp.symbol = ss.symbol;
