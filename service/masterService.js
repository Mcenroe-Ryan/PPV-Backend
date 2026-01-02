const { query } = require("../config/db");
const dayjs = require("dayjs");
const customParseFormat = require("dayjs/plugin/customParseFormat");
dayjs.extend(customParseFormat);

const getAllSuppliers = async () => {
  try {
    const result = await query("select * from suppliers");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getAllSuppliersData = async (skuId) => {
  const sql = `
    SELECT DISTINCT ON (sp.supplier_id)
           sp.supplier_id,
           sp.supplier_name,
           sp.supplier_country
    FROM public.supplier_plant_mapping AS spm
    JOIN public.suppliers AS sp
      ON sp.supplier_id = spm.supplier_id
    WHERE spm.sku_id = $1
    ORDER BY sp.supplier_id, spm.mapping_id ASC;
  `;
  try {
    const { rows } = await query(sql, [Number(skuId)]);
    return rows;
  } catch (err) {
    console.error("Database error (getAllSuppliersData):", err);
    throw err;
  }
};

const getAllCities = async () => {
  try {
    const result = await query("select * from dim_city where state_id = 1");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getAllPlants = async () => {
  try {
    const result = await query("select * from dim_plant where city_id = 2");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getAllCategories = async () => {
  try {
    const result = await query("select * from dim_category");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getAllSkus = async () => {
  try {
    const result = await query("select * from skus");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getAllSupplierLocation = async () => {
  try {
    const result = await query("select * from supplier_plant_mapping");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getSupplierSavingsLast6Months = async () => {
  const sql = `
    SELECT
      s.supplier_name,
      SUM(pfm.ppv_variance_amount) AS total_savings_dollars
    FROM ppv_forecast_monthly pfm
    JOIN suppliers s ON pfm.supplier_id = s.supplier_id
    WHERE pfm.forecast_month >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY s.supplier_name
    ORDER BY total_savings_dollars DESC;
  `;
  try {
    const { rows } = await query(sql);
    return rows;
  } catch (err) {
    console.error("Database error (getSupplierSavingsLast6Months):", err);
    throw err;
  }
};

const getLineChart = async ({ startDate, endDate }) => {
  const start = startDate || "2023-10-01";
  const end = endDate || "2025-09-01";

  const sql = `
    SELECT
      s.supplier_name,
      pfm.forecast_month,
      pfm.ppv_variance_percentage
    FROM ppv_forecast_monthly pfm
    JOIN suppliers s ON pfm.supplier_id = s.supplier_id
    WHERE pfm.forecast_month >= $1
      AND pfm.forecast_month <= $2
    ORDER BY s.supplier_name, pfm.forecast_month;
  `;

  try {
    const { rows } = await query(sql, [start, end]);
    return rows;
  } catch (err) {
    console.error("Database error (getLineChart):", err);
    throw err;
  }
};

const getWeeklyLineChart = async ({ startDate, endDate }) => {
  const start = startDate || "2023-10-01";
  const end = endDate || "2025-09-01";

  const sql = `
    SELECT
      s.supplier_name,
      pfw.forecast_week,
      pfw.ppv_variance_percentage
    FROM ppv_forecast_weekly pfw
    JOIN suppliers s ON pfw.supplier_id = s.supplier_id
    WHERE pfw.forecast_week >= $1
      AND pfw.forecast_week <= $2
    ORDER BY s.supplier_name, pfw.forecast_week;
  `;

  try {
    const { rows } = await query(sql, [start, end]);
    return rows;
  } catch (err) {
    console.error("Database error (getWeeklyLineChart):", err);
    throw err;
  }
};

const getAlerts = async () => {
  const sql = `
SELECT 
    a.alert_id,
    a.alert_type,
    a.trigger_date AS date_value,
    a.description AS tooltip,
    a.severity,
    s.supplier_name,
    p.plant_name,
    sk.sku_name,
    CASE 
        WHEN a.severity = 'Critical' THEN '🔴'
        WHEN a.severity = 'Warning' THEN '🟠'
        ELSE '🔵'
    END AS marker_color_emoji
FROM alerts a
JOIN suppliers s ON a.supplier_id = s.supplier_id
JOIN plants p ON a.plant_id = p.plant_id
JOIN skus sk ON a.sku_id = sk.sku_id
WHERE a.is_resolved = FALSE
ORDER BY a.trigger_date DESC;
  `;
  try {
    const { rows } = await query(sql);
    return rows;
  } catch (err) {
    console.error("Database error (getLineChart):", err);
    throw err;
  }
};

const getMonthlyPPVAlerts = async () => {
  const sql = `
WITH current_month_ppv AS (
    SELECT 
        pfw.supplier_id,
        s.supplier_name,
        s.supplier_country,
        COUNT(*) as total_weeks,
        AVG(pfw.ppv_variance_percentage) as avg_variance_percentage,
        SUM(pfw.ppv_variance_amount) as total_variance_amount,
        MIN(pfw.ppv_variance_percentage) as min_variance_percentage,
        MAX(pfw.ppv_variance_percentage) as max_variance_percentage
    FROM 
        public.ppv_forecast_weekly pfw
    INNER JOIN 
        public.suppliers s ON pfw.supplier_id = s.supplier_id
    WHERE 
        DATE_TRUNC('month', pfw.forecast_week) = DATE_TRUNC('month', CURRENT_DATE  - INTERVAL '1 month')
        AND pfw.forecast_type = 'actual'
    GROUP BY 
        pfw.supplier_id, 
        s.supplier_name,
        s.supplier_country
)
SELECT 
    supplier_id,
    supplier_name,
    supplier_country,
    total_weeks,
    ROUND(avg_variance_percentage, 2) as avg_variance_pct,
    ROUND(total_variance_amount, 2) as total_variance_amt,
    ROUND(min_variance_percentage, 2) as min_variance_pct,
    ROUND(max_variance_percentage, 2) as max_variance_pct,
    '🔴 NOT RECOMMENDED - Positive Variance' as alert_status
FROM 
    current_month_ppv
WHERE 
    avg_variance_percentage > 0
ORDER BY 
    avg_variance_percentage ASC;
  `;

  const { rows } = await query(sql);
  return rows;
};

const getWeeklyPPVAlerts = async () => {
  const sql = `
WITH current_week_ppv AS (
    SELECT 
        pfw.supplier_id,
        s.supplier_name,
        s.supplier_country,
        pfw.forecast_week,
        AVG(pfw.ppv_variance_percentage) as avg_variance_percentage,
        SUM(pfw.ppv_variance_amount) as total_variance_amount,
        COUNT(DISTINCT pfw.sku_id) as affected_skus_count,
        COUNT(DISTINCT pfw.plant_id) as affected_plants_count
    FROM 
        public.ppv_forecast_weekly pfw
    INNER JOIN 
        public.suppliers s ON pfw.supplier_id = s.supplier_id
    WHERE 
        DATE_TRUNC('week', pfw.forecast_week) = DATE_TRUNC('week', CURRENT_DATE)
        AND pfw.forecast_type = 'actual'
    GROUP BY 
        pfw.supplier_id, 
        s.supplier_name,
        s.supplier_country,
        pfw.forecast_week
)
SELECT 
    supplier_id,
    supplier_name,
    supplier_country,
    forecast_week as week_date,
    EXTRACT(WEEK FROM forecast_week) as week_number,
    EXTRACT(YEAR FROM forecast_week) as year,
    affected_skus_count,
    affected_plants_count,
    ROUND(avg_variance_percentage, 2) as avg_variance_pct,
    ROUND(total_variance_amount, 2) as total_variance_amt,
    '🔴 NOT RECOMMENDED - Negative Variance' as alert_status
FROM 
    current_week_ppv
WHERE 
    avg_variance_percentage < 0
ORDER BY 
    avg_variance_percentage ASC;
  `;

  const { rows } = await query(sql);
  return rows;
};

const getGlobalEvents = async () => {
  const sql = `
SELECT 
    ge.event_name AS label,
    ge.event_date AS date_value,
    ge.description AS tooltip,
    ge.impact_level,
    c.country_code AS country_flag, -- e.g., '🇺🇸' or '🇮🇳'
    c.country_name
FROM global_events ge
JOIN countries c ON ge.country_id = c.country_id
WHERE ge.is_active = TRUE
ORDER BY ge.event_date DESC;
  `;
  try {
    const { rows } = await query(sql);
    return rows;
  } catch (err) {
    console.error("Database error (getLineChart):", err);
    throw err;
  }
};

const getHeatMap = async (payload = {}) => {
  const {
    startDate,
    endDate,
    countryIds = [],
    stateIds = [],
    plantIds = [],
    skuIds = [],
    supplierIds = [],
    supplierLocations = [],
  } = payload;

  const start = startDate || "2023-10-01";
  const end = endDate || "2026-12-01";

  const sql = `
    SELECT
      s.supplier_name,
      pfm.forecast_month::date                         AS month_date,
      to_char(pfm.forecast_month, 'Mon YYYY')          AS month_label,
      pfm.ppv_variance_percentage::numeric             AS pct
    FROM ppv_forecast_monthly pfm
    JOIN suppliers s       ON s.supplier_id = pfm.supplier_id
    LEFT JOIN plants   p   ON p.plant_id     = pfm.plant_id
    LEFT JOIN states   st  ON st.state_id    = p.state_id
    LEFT JOIN countries c  ON c.country_id   = st.country_id
    WHERE pfm.forecast_month BETWEEN $1::date AND $2::date
      AND (cardinality($3::int[]) = 0 OR c.country_id = ANY($3::int[]))
      AND (cardinality($4::int[]) = 0 OR st.state_id  = ANY($4::int[]))
      AND (cardinality($5::int[]) = 0 OR pfm.plant_id = ANY($5::int[]))
      AND (cardinality($6::int[]) = 0 OR pfm.sku_id   = ANY($6::int[]))
      AND (cardinality($7::int[]) = 0 OR pfm.supplier_id = ANY($7::int[]))
      AND (cardinality($8::text[]) = 0 OR s.supplier_country = ANY($8::text[]))
    ORDER BY s.supplier_name, pfm.forecast_month
  `;

  const params = [
    start,
    end,
    countryIds,
    stateIds,
    plantIds,
    skuIds,
    supplierIds,
    supplierLocations,
  ];

  try {
    const { rows } = await query(sql, params);

    const bySupplier = new Map();
    for (const r of rows) {
      if (!bySupplier.has(r.supplier_name)) {
        bySupplier.set(r.supplier_name, { supplier_name: r.supplier_name });
      }
      const obj = bySupplier.get(r.supplier_name);
      obj[r.month_label] = r.pct;
    }

    return Array.from(bySupplier.values()).sort((a, b) =>
      a.supplier_name.localeCompare(b.supplier_name)
    );
  } catch (err) {
    console.error("Database error (getHeatMap):", err);
    throw err;
  }
};

const getWeeklyHeatMap = async (payload = {}) => {
  const {
    startDate,
    endDate,
    countryIds = [],
    stateIds = [],
    plantIds = [],
    skuIds = [],
    supplierIds = [],
    supplierLocations = [],
  } = payload;

  const start = startDate || "2023-10-01";
  const end = endDate || "2026-12-31";

  const sql = `
    SELECT
      s.supplier_name,
      pfw.forecast_week::date                      AS week_date,
      TO_CHAR(pfw.forecast_week, 'Mon DD, YYYY')   AS week_label,
      pfw.ppv_variance_percentage::numeric         AS pct
    FROM ppv_forecast_weekly pfw
    JOIN suppliers s       ON s.supplier_id = pfw.supplier_id
    LEFT JOIN plants   p   ON p.plant_id     = pfw.plant_id
    LEFT JOIN states   st  ON st.state_id    = p.state_id
    LEFT JOIN countries c  ON c.country_id   = st.country_id
    WHERE pfw.forecast_week BETWEEN $1::date AND $2::date
      AND (CARDINALITY($3::int[]) = 0 OR c.country_id = ANY($3::int[]))
      AND (CARDINALITY($4::int[]) = 0 OR st.state_id = ANY($4::int[]))
      AND (CARDINALITY($5::int[]) = 0 OR pfw.plant_id = ANY($5::int[]))
      AND (CARDINALITY($6::int[]) = 0 OR pfw.sku_id = ANY($6::int[]))
      AND (CARDINALITY($7::int[]) = 0 OR pfw.supplier_id = ANY($7::int[]))
      AND (CARDINALITY($8::text[]) = 0 OR s.supplier_country = ANY($8::text[]))
    ORDER BY s.supplier_name, pfw.forecast_week
  `;

  const params = [
    start,
    end,
    countryIds,
    stateIds,
    plantIds,
    skuIds,
    supplierIds,
    supplierLocations,
  ];

  try {
    const { rows } = await query(sql, params);

    const bySupplier = new Map();
    for (const r of rows) {
      if (!bySupplier.has(r.supplier_name)) {
        bySupplier.set(r.supplier_name, { supplier_name: r.supplier_name });
      }
      const obj = bySupplier.get(r.supplier_name);
      obj[r.week_label] = r.pct;
    }

    return Array.from(bySupplier.values()).sort((a, b) =>
      a.supplier_name.localeCompare(b.supplier_name)
    );
  } catch (err) {
    console.error("Database error (getWeeklyHeatMap):", err);
    throw err;
  }
};

const getAllModels = async () => {
  try {
    const result = await query("select * from dim_models");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getAllEvents = async () => {
  try {
    const result = await query("select * from dim_event");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getAllAlertsAndErrors = async () => {
  try {
    const result = await query("select * from forecast_error");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getPlantsByCity = async (city_id) => {
  const result = await query("SELECT * FROM dim_plant WHERE city_id = $1", [
    city_id,
  ]);
  return result.rows;
};

const getCategoriesByPlant = async (plant_id) => {
  const result = await query("SELECT * FROM dim_category WHERE plant_id = $1", [
    plant_id,
  ]);
  return result.rows;
};

const getSkusByCategory = async (category_id) => {
  const result = await query("SELECT * FROM dim_sku WHERE category_id = $1", [
    category_id,
  ]);
  return result.rows;
};

const getForecastData = async (filters) => {
  const model_name = filters.model_name || "XGBoost";
  const start_date = filters.startDate;
  const end_date = filters.endDate;
  const whereClauses = ["model_name = $1", "item_date BETWEEN $2 AND $3"];
  const values = [model_name, start_date, end_date];
  let idx = 4;

  const filterMap = {
    country: "country_name",
    state: "state_name",
    cities: "city_name",
    plants: "plant_name",
    categories: "category_name",
    skus: "sku_code",
    channels: "channel_name",
  };

  for (const [inputKey, columnName] of Object.entries(filterMap)) {
    const val = filters[inputKey];
    if (val) {
      if (Array.isArray(val) && val.length > 0) {
        whereClauses.push(`${columnName} = ANY($${idx})`);
        values.push(val);
      } else if (typeof val === "string" || typeof val === "number") {
        whereClauses.push(`${columnName} = $${idx}`);
        values.push(val);
      }
      idx++;
    }
  }

  const queryText = `SELECT 
      sum(actual_units) as actual_units,
      sum(baseline_forecast) as baseline_forecast,
      sum(ml_forecast) as ml_forecast,
      sum(sales_units) as sales_units, 
      sum(promotion_marketing) as promotion_marketing,
      sum(consensus_forecast) as consensus_forecast,
      sum(revenue_forecast_lakhs) as revenue_forecast_lakhs,
      sum(inventory_level_pct) as inventory_level_pct,
      AVG(stock_out_days) as stock_out_days,
      sum(on_hand_units) as on_hand_units,
      AVG(mape) AS avg_mape,
      month_name
    FROM public.demand_forecast
    WHERE ${whereClauses.join(" AND ")}
    GROUP BY month_name
    ORDER BY TO_DATE(month_name, 'FMMonth YYYY')
  `;
  const result = await query(queryText, values);
  return result.rows;
};

const getWeekForecastData = async (filters) => {
  const model_name = filters.model_name || "XGBoost";
  const start_date = filters.startDate;
  const end_date = filters.endDate;
  const whereClauses = ["model_name = $1", "item_date BETWEEN $2 AND $3"];
  const values = [model_name, start_date, end_date];
  let idx = 4;

  const filterMap = {
    country: "country_name",
    state: "state_name",
    cities: "city_name",
    plants: "plant_name",
    categories: "category_name",
    skus: "sku_code",
    channels: "channel_name",
  };

  for (const [inputKey, columnName] of Object.entries(filterMap)) {
    const val = filters[inputKey];
    if (val) {
      if (Array.isArray(val) && val.length > 0) {
        whereClauses.push(`${columnName} = ANY($${idx})`);
        values.push(val);
      } else if (typeof val === "string" || typeof val === "number") {
        whereClauses.push(`${columnName} = $${idx}`);
        values.push(val);
      }
      idx++;
    }
  }

  const queryText = `SELECT 
      sum(actual_units) as actual_units,
      sum(baseline_forecast) as baseline_forecast,
      sum(ml_forecast) as ml_forecast,
      sum(sales_units) as sales_units, 
      sum(promotion_marketing) as promotion_marketing,
      sum(consensus_forecast) as consensus_forecast,
      sum(revenue_forecast_lakhs) as revenue_forecast_lakhs,
      sum(inventory_level_pct) as inventory_level_pct,
      AVG(stock_out_days) as stock_out_days,
      sum(on_hand_units) as on_hand_units,
      AVG(mape) AS avg_mape,
      week_name
    FROM public.weekly_demand_forecast
    WHERE ${whereClauses.join(" AND ")}
    GROUP BY week_name
    ORDER BY week_name
  `;

  const result = await query(queryText, values);
  return result.rows;
};

const getAllCountries = async () => {
  try {
    const result = await query("select * from countries");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getStatesByCountry = async (countryIds) => {
  if (!countryIds || countryIds.length === 0) return [];
  const placeholders = countryIds.map((_, i) => `$${i + 1}`).join(", ");
  const result = await query(
    `SELECT * FROM states WHERE country_id IN (${placeholders})`,
    countryIds
  );
  return result.rows;
};

const getPlantsByStates = async (stateIds) => {
  if (!stateIds || stateIds.length === 0) return [];
  const placeholders = stateIds.map((_, i) => `$${i + 1}`).join(", ");
  const result = await query(
    `SELECT * FROM plants WHERE state_id IN (${placeholders})`,
    stateIds
  );
  return result.rows;
};

const getSkuByPlants = async (plantIds) => {
  if (!plantIds || plantIds.length === 0) return [];
  const placeholders = plantIds.map((_, i) => `$${i + 1}`).join(", ");
  const result = await query(
    `SELECT * FROM skus WHERE plant_id IN (${placeholders})`,
    plantIds
  );
  return result.rows;
};

const getPlantsByCities = async (cityIds) => {
  if (!cityIds || cityIds.length === 0) return [];
  const placeholders = cityIds.map((_, i) => `$${i + 1}`).join(", ");
  const result = await query(
    `SELECT * FROM dim_plant WHERE city_id IN (${placeholders})`,
    cityIds
  );
  return result.rows;
};

const getSkusByCategories = async (categoryIds) => {
  if (!categoryIds || categoryIds.length === 0) return [];
  const placeholders = categoryIds.map((_, i) => `$${i + 1}`).join(", ");
  const result = await query(
    `SELECT * FROM dim_sku WHERE category_id IN (${placeholders})`,
    categoryIds
  );
  return result.rows;
};

const updateConsensusForecast = async (payload) => {
  const requiredParams = [
    "country_name",
    "state_name",
    "city_name",
    "plant_name",
    "category_name",
    "sku_code",
    "channel_name",
    "consensus_forecast",
    "target_month",
    "model_name",
  ];

  for (const param of requiredParams) {
    if (!(param in payload)) {
      console.error(`Missing required parameter: ${param}`);
      throw new Error(`Missing required parameter: ${param}`);
    }
  }
  let targetMonth;

  if (dayjs(payload.target_month, "YYYY-MM-DD", true).isValid()) {
    targetMonth = dayjs(payload.target_month, "YYYY-MM-DD")
      .endOf("month")
      .format("YYYY-MM-DD");
  } else {
    console.error(
      "Invalid target_month format. Received:",
      payload.target_month
    );
    throw new Error("target_month must be in 'YYYY-MM-DD' format");
  }

  const consensusValue = Number(payload.consensus_forecast);
  if (isNaN(consensusValue)) {
    console.error(
      "Invalid consensus_forecast value. Received:",
      payload.consensus_forecast
    );
    throw new Error("consensus_forecast must be a valid number");
  }

  const model_name = payload.model_name || "XGBoost";
  const arr = (v) => (Array.isArray(v) ? v : [v]);

  const params = [
    consensusValue,
    arr(payload.country_name),
    arr(payload.state_name),
    arr(payload.city_name),
    arr(payload.plant_name),
    arr(payload.category_name),
    arr(payload.sku_code),
    arr(payload.channel_name),
    model_name,
    targetMonth,
  ];

  const sql = `
    UPDATE public.demand_forecast
    SET consensus_forecast = $1
    WHERE country_name = ANY($2)
      AND state_name = ANY($3)
      AND city_name = ANY($4)
      AND plant_name = ANY($5)
      AND category_name = ANY($6)
      AND sku_code = ANY($7)
      AND channel_name = ANY($8)
      AND model_name = $9
      AND DATE(item_date) = $10
  `;
  try {
    const result = await query(sql, params);
    console.table(result.rows);
    return {
      success: true,
      message: `Updated ${result.rowCount} record(s) for consensus_forecast using model: ${model_name}.`,
      updatedCount: result.rowCount,
      modelUsed: model_name,
    };
  } catch (error) {
    console.error("Error updating consensus_forecast:", error);
    throw new Error("Failed to update consensus_forecast");
  }
};

const getForecastAlertData = async (filters) => {
  const model_name = filters.model_name;

  let start_date, end_date;

  if (filters.startDate && filters.endDate) {
    start_date = filters.startDate;
    end_date = filters.endDate;
  } else {
    const now = new Date();
    const sixMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 6, 1);

    let futureYear = now.getFullYear();
    let futureMonth = now.getMonth() + 6;
    if (futureMonth > 11) {
      futureYear += Math.floor(futureMonth / 12);
      futureMonth = futureMonth % 12;
    }
    const sixMonthsAhead = new Date(futureYear, futureMonth + 1, 0);

    start_date = sixMonthsAgo.toISOString().split("T")[0];
    end_date = sixMonthsAhead.toISOString().split("T")[0];
  }

  const whereClauses = [
    "model_name = $1",
    "sales_week_start BETWEEN $2 AND $3",
  ];
  const values = [model_name, start_date, end_date];
  let idx = 4;

  const filterMap = {
    country: "country_name",
    state: "state_name",
    cities: "city_name",
    plants: "plant_name",
    categories: "category_name",
    skus: "sku_code",
    channels: "channel_name",
  };

  for (const [inputKey, columnName] of Object.entries(filterMap)) {
    const val = filters[inputKey];
    if (val) {
      if (Array.isArray(val) && val.length > 0) {
        whereClauses.push(`${columnName} = ANY($${idx})`);
        values.push(val);
      } else if (typeof val === "string" || typeof val === "number") {
        whereClauses.push(`${columnName} = $${idx}`);
        values.push(val);
      }
      idx++;
    }
  }

  const queryText = `
    SELECT 
      actual_units,
      ml_forecast,
      sales_week_start
    FROM public.weekly_sales_forecast
    WHERE ${whereClauses.join(" AND ")}
    ORDER BY sales_week_start  
  `;

  const result = await query(queryText, values);
  return result.rows;
};

const getDsModels = async () => {
  try {
    const result = await query("select * from ds_model");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getDsModelsFeatures = async () => {
  try {
    const result = await query("select * from ds_model_features");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getDsModelMetrics = async () => {
  try {
    const result = await query("select * from ds_model_metric");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const getFvaVsStats = async () => {
  try {
    const result = await query("select * from fva_vs_stats");
    return result.rows;
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const alertCountService = async () => {
  try {
    const result = await query(
      "SELECT COUNT(*) AS error_count FROM forecast_error WHERE error_type = 'error'"
    );
    return Number(result.rows[0].error_count);
  } catch (err) {
    console.error("Database error:", err);
    throw err;
  }
};

const updateAlertsStrikethroughService = async (id, is_checked) => {
  const queryText = `
    UPDATE forecast_error
    SET is_checked = $1
    WHERE id = $2
    RETURNING *;
  `;
  const values = [is_checked, id];
  const result = await query(queryText, values);

  return result.rows[0];
};

const getSAQChartData = async (supplier_id, start_date, end_date) => {
  let effectiveSupplierId = Number(
    supplier_id ?? DEFAULT_SAQ_FILTERS.supplier_id
  );
  if (!Number.isFinite(effectiveSupplierId) || effectiveSupplierId <= 0) {
    effectiveSupplierId = DEFAULT_SAQ_FILTERS.supplier_id;
  }

  const effectiveStartDate = start_date || DEFAULT_SAQ_FILTERS.start_date;
  const effectiveEndDate = end_date || DEFAULT_SAQ_FILTERS.end_date;

  const sql = `
    WITH base AS (
      SELECT 
        pfm.forecast_month::date AS date_value,
        sp.standard_price,
        (sp.standard_price + pfm.ppv_variance_amount) AS actual_price,
        pq.quantity
      FROM ppv_forecast_monthly pfm
      JOIN LATERAL (
        SELECT standard_price
        FROM standard_prices
        WHERE supplier_id = pfm.supplier_id 
          AND plant_id = pfm.plant_id 
          AND sku_id = pfm.sku_id
          AND effective_date <= pfm.forecast_month
        ORDER BY effective_date DESC
        LIMIT 1
      ) sp ON true
      JOIN purchase_quantities pq ON (
        pq.supplier_id = pfm.supplier_id
        AND pq.plant_id = pfm.plant_id
        AND pq.sku_id = pfm.sku_id
        AND pq.forecast_month = pfm.forecast_month
      )
      WHERE pfm.supplier_id = $1
        AND pfm.forecast_month BETWEEN $2 AND $3
    )
    SELECT 
      date_value,
      ROUND(AVG(standard_price)::numeric, 4) AS standard_price,
      ROUND(AVG(actual_price)::numeric, 4) AS actual_price,
      ROUND(SUM(quantity)::numeric, 2) AS quantity
    FROM base
    GROUP BY date_value
    ORDER BY date_value;
  `;

  try {
    const { rows } = await query(sql, [
      effectiveSupplierId,
      effectiveStartDate,
      effectiveEndDate,
    ]);
    return rows;
  } catch (error) {
    throw error;
  }
};

const getSAQTableData = async (supplier_id, start_date, end_date) => {
  const sql = `
    WITH base AS (
      SELECT 
        pfm.forecast_month::date AS date_value,
        sp.standard_price,
        (sp.standard_price + pfm.ppv_variance_amount) AS actual_price,
        pq.quantity
      FROM ppv_forecast_monthly pfm
      JOIN LATERAL (
        SELECT standard_price
        FROM standard_prices
        WHERE supplier_id = pfm.supplier_id 
          AND plant_id = pfm.plant_id 
          AND sku_id = pfm.sku_id
          AND effective_date <= pfm.forecast_month
        ORDER BY effective_date DESC
        LIMIT 1
      ) sp ON true
      JOIN purchase_quantities pq ON (
        pq.supplier_id = pfm.supplier_id
        AND pq.plant_id = pfm.plant_id
        AND pq.sku_id = pfm.sku_id
        AND pq.forecast_month = pfm.forecast_month
      )
      WHERE pfm.supplier_id = $1
        AND pfm.forecast_month BETWEEN $2 AND $3
    )
    SELECT 
      EXTRACT(YEAR FROM date_value) AS year,
      TO_CHAR(date_value, 'Mon') AS month_label,
      ROUND(AVG(standard_price)::numeric, 2) AS standard_price,
      ROUND(AVG(actual_price)::numeric, 2) AS actual_price,
      ROUND(SUM(quantity)::numeric, 0) AS quantity
    FROM base
    GROUP BY date_value
    ORDER BY date_value;
  `;
  const { rows } = await query(sql, [supplier_id, start_date, end_date]);
  return rows;
};

const getSAQQuantityData = async (supplier_id, start_date, end_date) => {
  const sql = `
    WITH base AS (
      SELECT 
        pfm.forecast_month::date AS date_value,
        pq.quantity
      FROM ppv_forecast_monthly pfm
      JOIN purchase_quantities pq ON (
        pq.supplier_id = pfm.supplier_id
        AND pq.plant_id = pfm.plant_id
        AND pq.sku_id = pfm.sku_id
        AND pq.forecast_month = pfm.forecast_month
      )
      WHERE pfm.supplier_id = $1
        AND pfm.forecast_month BETWEEN $2 AND $3
    )
    SELECT 
      date_value,
      ROUND(SUM(quantity)::numeric, 2) AS quantity
    FROM base
    GROUP BY date_value
    ORDER BY date_value;
  `;
  const { rows } = await query(sql, [supplier_id, start_date, end_date]);
  return rows;
};

const getSupplierScorecards = async (scoreCategory, supplierId) => {
  const params = [];
  let sql = `
    SELECT 
      s.supplier_name,
      sc.pricing_score,
      sc.order_accuracy_score,
      sc.on_time_delivery_score,
      sc.good_service_quality_score,
      sc.risk_compliance_score,
      sc.overall_rc_score,
      sc.score_category,
      sc.scorecard_id
    FROM supplier_scorecards sc
    JOIN suppliers s ON sc.supplier_id = s.supplier_id
    WHERE sc.is_active = TRUE
  `;

  if (scoreCategory) {
    params.push(scoreCategory);
    sql += ` AND sc.score_category = $${params.length}`;
  }

  if (supplierId) {
    params.push(supplierId);
    sql += ` AND sc.supplier_id = $${params.length}`;
  }

  sql += ` ORDER BY sc.overall_rc_score DESC`;

  const result = await query(sql, params);
  return result.rows;
};

const getScorecardFactorsById = async (scorecardId) => {
  if (!scorecardId) return [];

  const result = await query(
    `
    SELECT
      factor_category,
      factor_name,
      factor_value
    FROM scorecard_factors
    WHERE scorecard_id = $1
    ORDER BY factor_category, factor_name
    `,
    [scorecardId]
  );

  return result.rows;
};

const getMarketMetricTrendsBySupplier = async (supplierId, metricName) => {
  if (!supplierId) return [];

  const params = [supplierId];
  let filterClause = "";

  const metricPatterns = {
    "fuel cost (brent)": ["fuel cost (brent)%", "fuel cost brent%"],
    "fuel cost (brent) in $": ["fuel cost (brent)%", "fuel cost brent%"],
    "fuel cost (wti)": ["fuel cost (wti)%", "fuel cost wti%"],
    "fuel cost (wti) in $": ["fuel cost (wti)%", "fuel cost wti%"],
    "exchange rate dollar/euro": [
      "exchange rate dollar/euro%",
      "exchange rate dollar euro%",
    ],
    "exchange rate dollar/yuan": [
      "exchange rate dollar/yuan%",
      "exchange rate dollar yuan%",
    ],
    "exchange rate dollar/rupee": [
      "exchange rate dollar/rupee%",
      "exchange rate dollar rupee%",
    ],
    "raw material - rubber / polymer cost in ($)": [
      "raw material - rubber / polymer cost%",
      "raw material - rubber cost%",
    ],
    quantity: ["quantity"],
  };

  if (metricName) {
    const key = metricName.toLowerCase().trim();
    const patterns = metricPatterns[key] || [`${key}%`];
    params.push(patterns);
    filterClause = ` AND metric_key LIKE ANY($${params.length})`;
  }

  const sql = `
    WITH mm AS (
      SELECT 
        trend_date AS date_value,
        value AS y_value,
        is_forecast,
        metric_name,
        LOWER(TRIM(metric_name)) AS metric_key
      FROM market_metric_trends
      WHERE supplier_id = $1
    )
    SELECT date_value, y_value, is_forecast, metric_name
    FROM mm
    WHERE 1=1
    ${filterClause}
    ORDER BY metric_key, date_value
  `;

  const result = await query(sql, params);
  return result.rows;
};

const getForecastExplainability = async (supplierId) => {
  if (!supplierId) return [];

  const sql = `
    SELECT 
      sf.category,
      sf.factor_name,
      sf.factor_value,
      sf.factor_weight,
      sf.factor_impact,
      CASE 
        WHEN sf.category = 'Financial' THEN 'Quantity'
        WHEN sf.category = 'Environmental & Social' THEN 'Raw Material - Rubber / Polymer Cost in ($)'
        WHEN sf.category = 'Regulatory & Legal' THEN 
          CASE 
            WHEN s.supplier_country = 'USA' THEN 'Fuel Cost (WTI) in $'
            ELSE 'Fuel Cost (Brent) in $'
          END
        WHEN sf.category = 'Operational' THEN 
          CASE 
            WHEN s.supplier_country = 'USA' THEN 'Exchange Rate Dollar/Euro'
            WHEN s.supplier_country = 'China' THEN 'Exchange Rate Dollar/Yuan'
            WHEN s.supplier_country = 'India' THEN 'Exchange Rate Dollar/Rupee'
          END
      END as chart_section
    FROM scorecard_factors sf
    JOIN supplier_scorecards ss ON ss.scorecard_id = sf.scorecard_id
    JOIN suppliers s ON s.supplier_id = ss.supplier_id
    WHERE ss.supplier_id = $1
    ORDER BY 
      CASE sf.category
        WHEN 'Financial' THEN 1
        WHEN 'Environmental & Social' THEN 2
        WHEN 'Regulatory & Legal' THEN 3
        WHEN 'Operational' THEN 4
      END,
      sf.factor_value DESC;
  `;

  const { rows } = await query(sql, [Number(supplierId)]);
  return rows;
};

const getQuantityTrendBySupplier = async (supplierId) => {
  if (!supplierId) return [];

  const result = await query(
    `
    SELECT 
      trend_date AS date_value,
      value AS y_value,
      is_forecast
    FROM market_metric_trends
    WHERE supplier_id = $1
      AND metric_name = 'Quantity'
    ORDER BY trend_date
    `,
    [supplierId]
  );

  return result.rows;
};

const getActiveScoringConfigurations = async () => {
  const result = await query(
    `
    SELECT 
      config_id,
      config_name,
      financial_weight,
      environmental_social_weight,
      regulatory_legal_weight,
      operational_weight,
      total_weight
    FROM scoring_configurations
    WHERE is_active = TRUE
    ORDER BY config_name
    `
  );

  return result.rows;
};

const getBottomMarketTrends = async () => {
  const result = await query(
    `
    SELECT 
      metric_name,
      trend_date AS date_value,
      value AS y_value
    FROM market_trends
    WHERE metric_name IN (
      'Quantity',
      'Raw Material - Rubber / Polymer Cost in ($)',
      'Fuel Cost (WTI) in $',
      'Fuel Cost (Brent) in $',
      'Exchange Rate Dollar/Euro'
    )
    ORDER BY metric_name, trend_date
    `
  );

  return result.rows;
};

const getScoreCategories = async () => {
  const result = await query(
    `
    SELECT DISTINCT 
      score_category AS value,
      score_category AS label
    FROM supplier_scorecards
    WHERE is_active = TRUE
    ORDER BY score_category
    `
  );

  return result.rows;
};

module.exports = {
  getAllCategories,
  getAllCities,
  getAllPlants,
  getAllSkus,
  getAllCountries,
  getStatesByCountry,
  getPlantsByCity,
  getCategoriesByPlant,
  getSkusByCategory,
  getForecastData,
  getPlantsByCities,
  getSkusByCategories,
  updateConsensusForecast,
  getAllModels,
  getAllEvents,
  getAllAlertsAndErrors,
  getForecastAlertData,
  alertCountService,
  updateAlertsStrikethroughService,
  getWeekForecastData,
  getDsModels,
  getDsModelsFeatures,
  getDsModelMetrics,
  getFvaVsStats,
  getPlantsByStates,
  getSkuByPlants,
  getAllSuppliers,
  getAllSupplierLocation,
  getSupplierSavingsLast6Months,
  getLineChart,
  getWeeklyLineChart,
  getHeatMap,
  getWeeklyHeatMap,
  getAlerts,
  getMonthlyPPVAlerts,
  getWeeklyPPVAlerts,
  getGlobalEvents,
  getAllSuppliersData,
  getSAQChartData,
  getSAQTableData,
  getSAQQuantityData,
  getSupplierScorecards,
  getScorecardFactorsById,
  getMarketMetricTrendsBySupplier,
  getForecastExplainability,
  getActiveScoringConfigurations,
  getBottomMarketTrends,
  getScoreCategories,
  getQuantityTrendBySupplier,
};
