const { query } = require("../config/db");

const {
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
  updateConsensusForecast,
  getAllModels,
  getAllEvents,
  getAllAlertsAndErrors,
  alertCountService,
  updateAlertsStrikethroughService,
  //compare model
  getDsModels,
  getDsModelsFeatures,
  getDsModelMetrics,
  getFvaVsStats,
  getAllSuppliers,
  getAllSupplierLocation,
  getSAQChartData,
  getSAQTableData,
  getSAQQuantityData,
  getForecastExplainability,
} = require("../service/masterService");

const getAllSupplierData = async (req, res) => {
  try {
    const result = await getAllSuppliers();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getAllCitiesData = async (req, res) => {
  try {
    const result = await getAllCities();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getAllCategoriesData = async (req, res) => {
  try {
    const result = await getAllCategories();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getAllSkusData = async (req, res) => {
  try {
    const result = await getAllSkus();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getAllSupplierLocationData = async (req, res) => {
  try {
    const result = await getAllSupplierLocation();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};
const getAllPlantsData = async (req, res) => {
  try {
    const result = await getAllPlants();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getAllCountriesData = async (req, res) => {
  try {
    const result = await getAllCountries();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getAllModelsData = async (req, res) => {
  try {
    const result = await getAllModels();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getAllEventsData = async (req, res) => {
  try {
    const result = await getAllEvents();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getAllAlertsAndErrorsData = async (req, res) => {
  try {
    const result = await getAllAlertsAndErrors();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

// GET /states?country_id=1
const fetchStates = async (req, res) => {
  try {
    const { country_id } = req.query;
    const states = await getStatesByCountry(country_id);
    res.json(states);
  } catch (error) {
    console.error(error);
    res.status(500).send("Error fetching states");
  }
};
// GET /cities?state_id=1
const fetchCities = async (req, res) => {
  try {
    const { state_id } = req.query;
    const cities = await getCitiesByState(state_id);
    res.json(cities);
  } catch (error) {
    console.error(error);
    res.status(500).send("Error fetching cities");
  }
};

// GET /plants?city_id=1
const fetchPlants = async (req, res) => {
  try {
    const { city_id } = req.query;
    const plants = await getPlantsByCity(city_id);
    res.json(plants);
  } catch (error) {
    console.error(error);
    res.status(500).send("Error fetching plants");
  }
};

// GET /categories?plant_id=1
const fetchCategories = async (req, res) => {
  try {
    const { plant_id } = req.query;
    const categories = await getCategoriesByPlant(plant_id);
    res.json(categories);
  } catch (error) {
    console.error(error);
    res.status(500).send("Error fetching categories");
  }
};

// GET /skus?category_id=1
const fetchSkus = async (req, res) => {
  try {
    const { category_id } = req.query;
    const skus = await getSkusByCategory(category_id);
    res.json(skus);
  } catch (error) {
    console.error(error);
    res.status(500).send("Error fetching SKUs");
  }
};

// GET /forecast?country_name=India&state_name=...&start_date=...&end_date=...
const fetchForecastData = async (req, res) => {
  try {
    const filters = req.query;
    const data = await getForecastData(filters);
    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).send("Error fetching forecast data");
  }
};
const getForecastDataController = async (req, res) => {
  try {
    const data = await getForecastDataForTest();
    res.json(data);
  } catch (err) {
    console.error("Forecast fetch error:", err);
    res.status(500).json({ message: "Failed to fetch Forecast" });
  }
};

const getPlantsByCities = async (city_ids) => {
  const result = await query(
    "SELECT * FROM dim_plant WHERE city_id = ANY($1)",
    [city_ids]
  );
  return result.rows;
};

const getDsModelData = async (req, res) => {
  try {
    const result = await getDsModels();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getDsModelsFeaturesData = async (req, res) => {
  try {
    const result = await getDsModelsFeatures();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getDsModelMetricsData = async (req, res) => {
  try {
    const result = await getDsModelMetrics();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getFvaVsStatsData = async (req, res) => {
  try {
    const result = await getFvaVsStats();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getAlertCountData = async (req, res) => {
  try {
    const result = await alertCountService();
    res.json(result);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const updateAlertsStrikethroughController = async (req, res) => {
  const { id } = req.params;
  const { in_checked } = req.body;

  if (typeof in_checked !== "boolean") {
    return res.status(400).json({ error: "`in_checked` must be a boolean." });
  }

  try {
    const updatedRow = await updateAlertsStrikethroughService(id, in_checked);

    if (!updatedRow) {
      return res.status(404).json({ error: "Record not found." });
    }

    res.json({ success: true, updated: updatedRow });
  } catch (err) {
    console.error("Update error:", err);
    res.status(500).json({ error: "Internal server error." });
  }
};

const getSAQChartDataController = async (req, res) => {
  try {
    const { supplier_id, start_date, end_date } = req.body;

    // Input validation
    if (!supplier_id || !start_date || !end_date) {
      return res.status(400).json({
        error: "Missing required parameters: supplier_id, start_date, end_date",
      });
    }

    const result = await getSAQChartData(supplier_id, start_date, end_date);
    res.status(200).json(result);
  } catch (error) {
    res.status(500).json({ error: "Internal server error" });
  }
};


const getSAQTableDataController = async (req, res) => {
  try {
    const { supplier_id, start_date, end_date } = req.query;
    const result = await getSAQTableData(supplier_id, start_date, end_date);
    res.json(result);
  } catch (err) {
    console.error("Error in getSAQTableData:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getSAQQuantityDataController = async (req, res) => {
  try {
    const { supplier_id, start_date, end_date } = req.query;
    const result = await getSAQQuantityData(supplier_id, start_date, end_date);
    res.json(result);
  } catch (err) {
    console.error("Error in getSAQQuantityData:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

const getForecastExplainabilityController = async (req, res) => {
  try {
    const supplierIdParam =
      req.params?.supplierId ?? req.query?.supplier_id ?? req.body?.supplier_id;
    const supplierId = Number(supplierIdParam);

    if (!Number.isFinite(supplierId) || supplierId <= 0) {
      return res
        .status(400)
        .json({ error: "supplierId is required and must be a number" });
    }

    const result = await getForecastExplainability(supplierId);
    res.json(result);
  } catch (err) {
    console.error("Error in getForecastExplainability:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
};

module.exports = {
  getAllCountriesData,
  getAllCitiesData,
  getAllCategoriesData,
  getAllSkusData,
  getAllPlantsData,
  fetchStates,
  fetchCities,
  fetchPlants,
  fetchCategories,
  fetchSkus,
  fetchForecastData,
  getForecastDataController,
  getPlantsByCities,
  getAllModelsData,
  getAllEventsData,
  getAllAlertsAndErrorsData,
  getAlertCountData,
  updateAlertsStrikethroughController,
  //Compare models
  getDsModelData,
  getDsModelsFeaturesData,
  getDsModelMetricsData,
  getFvaVsStatsData,

  getAllSupplierData,
  getAllSupplierLocationData,
  getSAQChartDataController,
  getSAQTableDataController,
  getSAQQuantityDataController,
  getForecastExplainabilityController
};
