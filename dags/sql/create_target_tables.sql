CREATE TABLE IF NOT EXISTS crypto
(
  	time_period_start date,
	time_period_end date,
	time_open date,
	time_close date,
	price_open DOUBLE PRECISION,
	price_high DOUBLE PRECISION,
	price_low DOUBLE PRECISION,
	price_close DOUBLE PRECISION,
	volume_traded DOUBLE PRECISION,
	trades_count integer
)