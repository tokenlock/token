class SpreadSizer:
    def __init__(self, default_quantity=2):
        self.prev_signal = None
        self.default_quantity = default_quantity

    # signal event中执行
    def calculate_quantity(self, signal):
        if signal != self.prev_signal:
            quantity = self.default_quantity
        else:
            quantity = 0
        return quantity