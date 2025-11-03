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
  // fallback window if dates not provided
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
// const getLineChart = async ({ startDate, endDate, skuId, skuIds }) => {
//   // fallback window if dates not provided
//   const start = startDate || "2023-10-01";
//   const end   = endDate   || "2025-09-01";

//   const params = [start, end];
//   const where = [
//     `pfm.forecast_month >= $1`,
//     `pfm.forecast_month <= $2`,
//   ];

//   // Optional SKU filters
//   // Accept either a single skuId or an array skuIds
//   if (Array.isArray(skuIds) && skuIds.length > 0) {
//     params.push(skuIds.map(Number)); // ensure ints
//     where.push(`pfm.sku_id = ANY($${params.length}::int[])`);
//   } else if (skuId != null) {
//     params.push(Number(skuId));
//     where.push(`pfm.sku_id = $${params.length}::int`);
//   }

//   const sql = `
//     SELECT
//       s.supplier_name,
//       pfm.forecast_month,
//       pfm.ppv_variance_percentage
//     FROM ppv_forecast_monthly pfm
//     JOIN suppliers s ON pfm.supplier_id = s.supplier_id
//     WHERE ${where.join(" AND ")}
//     ORDER BY s.supplier_name, pfm.forecast_month;
//   `;

//   try {
//     const { rows } = await query(sql, params);
//     return rows;
//   } catch (err) {
//     console.error("Database error (getLineChart):", err);
//     throw err;
//   }
// };
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


// const getHeatMap = async () => {
//   const sql = `
// SELECT
//     s.supplier_name,
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 10 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2023 THEN pfm.ppv_variance_percentage END) AS "Oct 2023",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 11 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2023 THEN pfm.ppv_variance_percentage END) AS "Nov 2023",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 12 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2023 THEN pfm.ppv_variance_percentage END) AS "Dec 2023",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 1 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "Jan 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 2 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "Feb 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 3 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "Mar 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 4 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "Apr 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 5 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "May 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 6 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "Jun 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 7 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "Jul 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 8 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "Aug 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 9 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "Sep 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 10 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "Oct 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 11 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "Nov 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 12 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2024 THEN pfm.ppv_variance_percentage END) AS "Dec 2024",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 1 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2025 THEN pfm.ppv_variance_percentage END) AS "Jan 2025",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 2 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2025 THEN pfm.ppv_variance_percentage END) AS "Feb 2025",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 3 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2025 THEN pfm.ppv_variance_percentage END) AS "Mar 2025",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 4 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2025 THEN pfm.ppv_variance_percentage END) AS "Apr 2025",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 5 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2025 THEN pfm.ppv_variance_percentage END) AS "May 2025",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 6 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2025 THEN pfm.ppv_variance_percentage END) AS "Jun 2025",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 7 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2025 THEN pfm.ppv_variance_percentage END) AS "Jul 2025",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 8 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2025 THEN pfm.ppv_variance_percentage END) AS "Aug 2025",
//     MAX(CASE WHEN EXTRACT(MONTH FROM pfm.forecast_month) = 9 AND EXTRACT(YEAR FROM pfm.forecast_month) = 2025 THEN pfm.ppv_variance_percentage END) AS "Sep 2025"
// FROM ppv_forecast_monthly pfm
// JOIN suppliers s ON pfm.supplier_id = s.supplier_id
// WHERE pfm.forecast_month BETWEEN '2023-10-01' AND '2025-09-01'
// GROUP BY s.supplier_name
// ORDER BY s.supplier_name;
//   `;
//   try {
//     const { rows } = await query(sql);
//     return rows;
//   } catch (err) {
//     console.error("Database error (getLineChart):", err);
//     throw err;
//   }
// };

// Accepts: { startDate, endDate, countryIds, stateIds, plantIds, skuIds, supplierIds, supplierLocations }
const getHeatMap = async (payload = {}) => {
  const {
    startDate,
    endDate,
    countryIds = [],
    stateIds = [],
    plantIds = [],
    skuIds = [],
    supplierIds = [],
    supplierLocations = [], // array of country names (strings)
  } = payload;

  // sensible fallbacks if FE didn't send dates
  const start = startDate || "2023-10-01";
  const end = endDate || "2026-12-01";

  // NOTE: adapt joins to your schema if country/state live elsewhere.
  // The idea is to keep every filter optional.
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

    // Pivot in Node: one object per supplier with dynamic "Mon YYYY" keys
    const bySupplier = new Map();
    for (const r of rows) {
      if (!bySupplier.has(r.supplier_name)) {
        bySupplier.set(r.supplier_name, { supplier_name: r.supplier_name });
      }
      const obj = bySupplier.get(r.supplier_name);
      obj[r.month_label] = r.pct; // e.g., "Apr 2026": 6.5
    }

    // Return array of pivoted rows, sorted by supplier
    return Array.from(bySupplier.values()).sort((a, b) =>
      a.supplier_name.localeCompare(b.supplier_name)
    );
  } catch (err) {
    console.error("Database error (getHeatMap):", err);
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

  // Map incoming filter keys to DB column names
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

  // Map incoming filter keys to DB column names
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
// Get States by Country (accepts array of state_ids)
const getStatesByCountry = async (countryIds) => {
  if (!countryIds || countryIds.length === 0) return [];
  const placeholders = countryIds.map((_, i) => `$${i + 1}`).join(", ");
  const result = await query(
    `SELECT * FROM states WHERE country_id IN (${placeholders})`,
    countryIds
  );
  return result.rows;
};

// Get Cities by State (accepts array of state_ids)
const getPlantsByStates = async (stateIds) => {
  if (!stateIds || stateIds.length === 0) return [];
  const placeholders = stateIds.map((_, i) => `$${i + 1}`).join(", ");
  const result = await query(
    `SELECT * FROM plants WHERE state_id IN (${placeholders})`,
    stateIds
  );
  return result.rows;
};

// Get Categories by Plant (accepts array of plant_ids)
const getSkuByPlants = async (plantIds) => {
  if (!plantIds || plantIds.length === 0) return [];
  const placeholders = plantIds.map((_, i) => `$${i + 1}`).join(", ");
  const result = await query(
    `SELECT * FROM skus WHERE plant_id IN (${placeholders})`,
    plantIds
  );
  return result.rows;
};

// Get Plants by City (accepts array of city_ids)
const getPlantsByCities = async (cityIds) => {
  if (!cityIds || cityIds.length === 0) return [];
  const placeholders = cityIds.map((_, i) => `$${i + 1}`).join(", ");
  const result = await query(
    `SELECT * FROM dim_plant WHERE city_id IN (${placeholders})`,
    cityIds
  );
  return result.rows;
};

// Get SKUs by Category (accepts array of category_ids)
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

  // 1. Validate required fields
  for (const param of requiredParams) {
    if (!(param in payload)) {
      console.error(`Missing required parameter: ${param}`);
      throw new Error(`Missing required parameter: ${param}`);
    }
  }

  // 2. Parse and validate target_month, then convert to month-end
  let targetMonth;

  if (dayjs(payload.target_month, "YYYY-MM-DD", true).isValid()) {
    // Convert beginning of month to end of month since backend stores month-end dates
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

  // 3. Validate and parse consensus_forecast
  const consensusValue = Number(payload.consensus_forecast);
  if (isNaN(consensusValue)) {
    console.error(
      "Invalid consensus_forecast value. Received:",
      payload.consensus_forecast
    );
    throw new Error("consensus_forecast must be a valid number");
  }

  const model_name = payload.model_name || "XGBoost"; // Default fallback
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
    model_name, //
    targetMonth,
  ];

  // 5. SQL query (unchanged)
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
    console.table(result.rows); // see exactly which rows changed and how

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

//compare model queries
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
    // result.rows[0].error_count will be the count as a string, so convert to number if needed
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

module.exports = {
  // demand_planning code
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
  //compare model
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
  getHeatMap,
  getAlerts,
  getGlobalEvents,
};
