CREATE TABLE dayAheadMarketTR(
	ID int IDENTITY(1,1) NOT NULL,
	TARIH date NOT NULL,
	TARIH_INT int NOT NULL,
	SAAT int NOT NULL,
	WEEKDAY int NULL,
	PTF_TR numeric(18, 4) NULL,
	PTF_EUR numeric(18, 4) NULL,
	PTF_USD numeric(18, 4) NULL,
	LEP numeric(18, 4) NULL,
	DPP numeric(18, 4) NULL,
	TRADEVOLUME numeric(18, 4) NULL,
	MATCHEDQUANTITY numeric(18, 4) NULL,
	TEMPERATURE numeric(18, 4) NULL,
	HUMIDITY numeric(18, 4) NULL,
	WINDSPEED numeric(18, 4) NULL
) 