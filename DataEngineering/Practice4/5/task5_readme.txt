Описание предметной области: 
Набор данных «Казначейские отчетные курсы валют» содержит официальные обменные курсы правительства США, чтобы обеспечить единообразие единиц измерения иностранной валюты и эквивалентов в долларах США во всей отчетности, предоставляемой государственными учреждениями. Этот отчет охватывает все иностранные валюты, в которых заинтересовано правительство США, включая: поступления и выплаты, начисленные доходы и расходы, разрешения, обязательства, дебиторскую и кредиторскую задолженность, возврат средств и аналогичные операции. Министр финансов обладает исключительными полномочиями устанавливать обменные курсы для всех иностранных валют или кредитов, о которых сообщают государственные учреждения в соответствии с федеральным законодательством. Для получения конкретных обменных курсов в зависимости от страны или валюты.

Описание таблиц:

Countries: Представляет страны, для которых фиксируются обменные курсы валют
  id (INTEGER, PRIMARY KEY, AUTOINCREMENT): Уникальный идентификатор записи. Генерируется автоматически.
  country_name(TEXT UNIQUE): Название страны. Уникальное значение.

Currencies: Представляет валюты, используемые в разных странах.
  id (INTEGER, PRIMARY KEY, AUTOINCREMENT): Уникальный идентификатор записи. Генерируется автоматически.
  currency_description (TEXT UNIQUE): Описание валюты. Уникальное значение.
  
ExchangeRates: Хранит данные об обменных курсах валют.
  id (INTEGER, PRIMARY KEY, AUTOINCREMENT): Уникальный идентификатор записи. Генерируется автоматически.
  record_date (TEXT): Дата записи обменного курса. Формат: YYYY-MM-DD.
  country_id (INTEGER): Внешний ключ, ссылается на поле id таблицы Countries. Определяет страну, для которой зафиксирован курс.
  currency_id (INTEGER): Внешний ключ, ссылается на поле id таблицы Currencies. Определяет валюту, для которой указан курс.
  exchange_rate (REAL): Обменный курс валюты относительно базовой валюты (например, USD).
  effective_date (TEXT): Дата, начиная с которой курс становится актуальным. Формат: YYYY-MM-DD.
  