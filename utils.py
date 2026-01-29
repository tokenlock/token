# 计算MACD
# MACDFS是1m k线收盘价当天尺度范围计算结果,与同花顺分时盘结果一致
# 同花顺当天分时盘MACDFS与自己的1m k线结果有轻微差异,是因为采用更多天范围的1m k线数据
def macd(df, dea_col='dea', dif_col='dif', hist_col='macd', fast_window=12, slow_window=26, signal_window=9, inplace=False):
    """
    计算MACD指标,并更新DataFrame
    
    参数:
    df : DataFrame
        包含价格数据的DataFrame,必须包含'open', 'high', 'low', 'close'列
    fast_window : int, optional
        快速EMA的窗口大小,默认为12
    slow_window : int, optional
        慢速EMA的窗口大小,默认为26
    signal_window : int, optional
        信号线的窗口大小,默认为9
    inplace : bool, optional
        是否在原地更新DataFrame,默认为False
    
    返回:
    df : DataFrame
        包含MACD指标的新DataFrame（如果inplace为False）
    """
    
    # 复制DataFrame以避免修改原始数据
    if not inplace:
        df = df.copy()
    
    # 计算快速EMA
    fast_ema = df['close'].ewm(span=fast_window, adjust=False).mean()
    
    # 计算慢速EMA
    slow_ema = df['close'].ewm(span=slow_window, adjust=False).mean()
    
    # 计算MACD线
    df[dif_col] = fast_ema - slow_ema
    
    # 计算信号线
    df[dea_col] = df[dif_col].ewm(span=signal_window, adjust=False).mean()
    
    # 计算MACD柱
    df[hist_col] = 2 * (df[dif_col] - df[dea_col])
    
    return df

# RSIFS采用1m k线当天尺度范围计算结果 
# 同花顺分时盘不显示RSIFS,腾讯自选股显示RSIFS, 同时与同花顺1m k线RSI结果一致按照1m k线
# 在开盘时 腾讯自选股RSIFS因为需要统计数据计算,前几分钟的数据是空
# 在1m k线上计算的结果是相同的
def rsi(df, rsi_col='rsi', window=12, inplace=False):
    """
    计算RSI指标,并更新DataFrame
    
    参数:
    df : DataFrame
        包含价格数据的DataFrame,必须包含'open', 'high', 'low', 'close'列
    window : int, optional
        RSI指标的窗口大小,默认为12
    inplace : bool, optional
        是否在原地更新DataFrame,默认为False
    
    返回:
    df : DataFrame
        包含RSI指标的新DataFrame（如果inplace为False）
    """
    
    # 复制DataFrame以避免修改原始数据
    if not inplace:
        df = df.copy()
    
    # 计算价格变动 后一天收盘价减前一天 错位相减
    diff = df['close'].diff()

    # 计算上涨和下跌的平均值
    up_avg = diff.apply(lambda x: max(x, 0)).ewm(com=window-1, min_periods=window).mean()
    down_avg = diff.apply(lambda x: abs(min(x, 0))).ewm(com=window-1, min_periods=window).mean()

    # 防止除以零
    up_avg.fillna(0, inplace=True)
    down_avg.fillna(0, inplace=True)

    # 计算RSI
    df[rsi_col] = 100 - (100 / (1 + (up_avg / down_avg)))
    
    return df

## talib函数结果与上述代码计算结果一致
## talib.set_compatibility(1)
## print(talib.get_compatibility())
## macd = talib.MACD(df["close"], 12, 26, 9)
## rsi_res = talib.RSI(df["close"], 12)

#计算MA 根据传入数据period和window不同 计算window日k 月k 
def ma(df, ma_col="ma", window=5, inplace=False):
    """
    计算MA指标,并更新DataFrame
    
    参数:
    df : DataFrame
        包含价格数据的DataFrame,必须包含'open', 'high', 'low', 'close'列
    window : int, optional
        MA的窗口大小,默认为5
    inplace : bool, optional
        是否在原地更新DataFrame,默认为False
    
    返回:
    df : DataFrame
        包含MA指标的新DataFrame（如果inplace为False）
    """    
    # 复制DataFrame以避免修改原始数据
    if not inplace:
        df = df.copy()
    # 计算快速K线MA
    kma = df['close'].rolling(window=window, min_periods=1).mean()
    df[ma_col] = kma
    return df

# 计算分时MA
def mafs(df, ma_col="mafs", inplace=False):
    """
    计算MA分时指标,并更新DataFrame
    
    参数:
    df : DataFrame
        包含价格数据的DataFrame,必须包含'open', 'high', 'low', 'close'列
    inplace : bool, optional
        是否在原地更新DataFrame,默认为False
    
    返回:
    df : DataFrame
        包含MA分时指标的新DataFrame（如果inplace为False）
    """    
    # 复制DataFrame以避免修改原始数据
    if not inplace:
        df = df.copy()
    # A股100股组成per volume, cumsum按分时累加交易量和交易额
    df[ma_col] = df["turnover"].cumsum() / df["volume"].cumsum() / 100
    return df

class MACDCalculator:
    def __init__(self, fast=12, slow=26, signal=9):
        if fast >= slow:
            raise ValueError("fast period must be smaller than slow period")
        self.fast = fast
        self.slow = slow
        self.signal = signal
        self.alpha_fast = 2 / (fast + 1)
        self.alpha_slow = 2 / (slow + 1)
        self.alpha_signal = 2 / (signal + 1)
        self.reset()

    def reset(self):
        self.ema_fast = None
        self.ema_slow = None
        self.dea = 0
        self.history = []

    def update(self, price):
        if self.ema_fast is None:
            self.ema_fast = price
            self.ema_slow = price
        self.ema_fast = self.alpha_fast * price + (1 - self.alpha_fast) * self.ema_fast
        self.ema_slow = self.alpha_slow * price + (1 - self.alpha_slow) * self.ema_slow
        diff = self.ema_fast - self.ema_slow
        self.dea = self.alpha_signal * diff + (1 - self.alpha_signal) * self.dea
        # histogram = diff - self.dea
        macd = 2 * (diff - self.dea)
        self.history.append((diff, self.dea, macd))
        return diff, self.dea, macd

class MAFSCalculator:
    """
    Calculates Money Flow Average per Share (MAFS) - the volume-weighted average price.
    
    Features:
    - Accumulates turnover and volume
    - Handles division by zero
    - Input validation
    - Reset capability
    - Ticks counting
    """
    
    def __init__(self):
        """Initialize a new MAFS calculator"""
        self.reset()
        
    def reset(self):
        """Reset all accumulated values"""
        self.turnover = 0.0  # Cumulative turnover in yuan
        self.volume = 0.0    # Cumulative volume in shares
        self.ticks = 0       # Number of updates received
        
    def update(self, turnover, volume):
        """
        Update calculations with new tick data
        
        Args:
            turnover: transaction amount in yuan (must be >= 0)
            volume: transaction volume in shares (must be >= 0)
            
        Returns:
            Current MAFS value or 0 if no volume
            
        Raises:
            ValueError: if negative values are provided
        """
        if volume < 0 or turnover < 0:
            raise ValueError("Turnover and volume must be non-negative")
            
        self.turnover += float(turnover)
        self.volume += float(volume)
        self.ticks += 1
        
        if self.volume == 0:
            return 0.0
            
        return self.turnover / self.volume
        
    def current_value(self):
        """Get current MAFS without updating"""
        if self.volume == 0:
            return 0.0
        return self.turnover / self.volume
    
class RSICalculator:
    def __init__(self, period=12):
        """
        RSI计算器
        :param period: 计算周期
        """
        self.period = period
        self.prices = []
        self.avg_gain = 0
        self.avg_loss = 0
        self.rsi = None
        self.initialized = False
    
    def update(self, price):
        """
        更新RSI值
        :param price: 最新价格
        :return: 当前RSI值
        """
        self.prices.append(price)
        
        if len(self.prices) < 2:
            return None  # 需要至少2个价格才能计算变化
        
        delta = self.prices[-1] - self.prices[-2]
        gain = delta if delta > 0 else 0
        loss = -delta if delta < 0 else 0
        
        if not self.initialized:
            if len(self.prices) == self.period + 1:
                # 初始化平均值
                deltas = [self.prices[i+1] - self.prices[i] for i in range(self.period)]
                gains = [d if d > 0 else 0 for d in deltas]
                losses = [-d if d < 0 else 0 for d in deltas]
                
                self.avg_gain = sum(gains) / self.period
                self.avg_loss = sum(losses) / self.period
                self.initialized = True
                
                rs = self.avg_gain / self.avg_loss if self.avg_loss != 0 else float('inf')
                self.rsi = 100 - (100 / (1 + rs))
        else:
            # 平滑移动平均
            self.avg_gain = (self.avg_gain * (self.period-1) + gain) / self.period
            self.avg_loss = (self.avg_loss * (self.period-1) + loss) / self.period
            
            rs = self.avg_gain / self.avg_loss if self.avg_loss != 0 else float('inf')
            self.rsi = 100 - (100 / (1 + rs))
        
        return self.rsi
    
    def reset(self):
        """重置计算器状态"""
        self.prices = []
        self.avg_gain = 0
        self.avg_loss = 0
        self.rsi = None
        self.initialized = False
