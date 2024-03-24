link: https://cmtassociation.org/wp-content/uploads/2018/05/2018_dowaward-giordano.pdf
     - basically using a passive fund with a quantitative strategy to outperform the market
     - the basis is that the success of passive funds comes from low volatility
     - 

1.  7twelve portfolio
    - passive portfolio of ETFs, equal allocation
    - multi asset
    - 7 core asset classes (equities (us/int), real estate, resources, bonds (us/int), cash)
    - represents a low cost indexed passive fund
2. volatility model
    - modified GARCH
    - OHLC daily of vix
3. ATR trend/breakout systemA
    - algo to identify breakouts
    - based on price and vol
    - signal on t, action on t+1
4. Ranking models (monthly)
    - absolute momentum
        - 4 months on daily returns
    - vol
        - 10 day auto regressive
    - average relative correlations
        - 4 months average correllation across etfs
    - atr
    - etfs are ranked 1 to 11 in ascending order
    - TOTAL RANK = (wM*Rank(M)+wV*Rank(V)+wC*Rank(C)-T)+M/x


# TODOs
- [x] yahoo data
- [ ] fmp data
- [/] replicate 7twelve
- [ ] momentum data - 4months daily returns
- [ ] basic backtesting model
- [ ] momentum
- [ ] rankings for momentum
- [ ] ATR
- [ ] volatility
- [ ] rankings
