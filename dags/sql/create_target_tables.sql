CREATE TABLE IF NOT EXISTS crypto
(
  	time_period_start date,
	time_period_end date,
	time_open date,
	time_close date,
	price_open NUMERIC(10,5),
	price_high NUMERIC(10,5),
	price_low NUMERIC(10,5),
	price_close NUMERIC(10,5),
	volume_traded NUMERIC(10,5),
	trades_count integer
)