const express = require("express");
const router = express.Router();
const {
  getAllStateData,
  getAllCategoriesData,
  getAllChannelsData,
  getAllCitiesData,
  getAllSkusData,
  getAllPlantsData,
  fetchStates,
  fetchCities,
  getAllCountriesData,
  fetchPlants,
  fetchCategories,
  fetchSkus,
  fetchForecastData,
  getForecastDataController,
  getAllModelsData,
  getAllEventsData,
  getAllAlertsAndErrorsData,
  getAlertCountData,
  updateAlertsStrikethroughController,
  //compare model
  getDsModelData,
  getDsModelsFeaturesData,
  getDsModelMetricsData,
  getFvaVsStatsData,
} = require("../controllers/masterController");
const service = require("../service/masterService");

// Basic GET routes
router.get("/getAllState", getAllStateData);
router.get("/getAllCategories", getAllCategoriesData);
router.get("/getAllChannels", getAllChannelsData);
router.get("/getAllCities", getAllCitiesData);
router.get("/getAllSkus", getAllSkusData);
router.get("/getAllPlants", getAllPlantsData);
router.get("/getAllCountries", getAllCountriesData);
router.get("/states", fetchStates);
router.get("/plants", fetchPlants);
router.get("/categories", fetchCategories);
router.get("/skus", fetchSkus);
router.get("/forecast", fetchForecastData);
router.get("/models", getAllModelsData);
router.get("/events", getAllEventsData);
router.get("/getAllAlerts", getAllAlertsAndErrorsData);
router.get("/getAlertCount", getAlertCountData);
router.put("/forecast-error/:id", updateAlertsStrikethroughController);

// Get Routes for Compare Models
router.get("/getDsModelData", getDsModelData);
router.get("/getDsModelFeaturesData", getDsModelsFeaturesData);
router.get("/getDsModelMetricsData", getDsModelMetricsData);
router.get("/getFvaVsStatsData", getFvaVsStatsData);

// POST routes
router.post("/forecast-test", getForecastDataController);

// Relationship-based routes
router.post("/states-by-country", async (req, res) => {
  try {
    const { countryIds } = req.body;
    if (!Array.isArray(countryIds) || countryIds.length === 0) {
      return res.json([]);
    }
    const states = await service.getStatesByCountry(countryIds);
    res.json(states);
  } catch (err) {
    console.error("Error in /states-by-country:", err);
    res.status(500).json({ error: err.message });
  }
});

router.post("/cities-by-states", async (req, res) => {
  try {
    const { stateIds } = req.body;
    if (!Array.isArray(stateIds) || stateIds.length === 0) {
      return res.json([]);
    }
    const cities = await service.getCitiesByStates(stateIds);
    res.json(cities);
  } catch (err) {
    console.error("Error in /cities-by-states:", err);
    res.status(500).json({ error: err.message });
  }
});

router.post("/plants-by-cities", async (req, res) => {
  try {
    const { cityIds } = req.body;
    if (!Array.isArray(cityIds) || cityIds.length === 0) {
      return res.json([]);
    }
    const plants = await service.getPlantsByCities(cityIds);
    res.json(plants);
  } catch (err) {
    console.error("Error in /plants-by-cities:", err);
    res.status(500).json({ error: err.message });
  }
});

router.post("/categories-by-plants", async (req, res) => {
  try {
    const { plantIds } = req.body;
    const categories = await service.getCategoriesByPlants(plantIds);
    res.json(categories);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post("/skus-by-categories", async (req, res) => {
  try {
    const { categoryIds } = req.body;
    const skus = await service.getSkusByCategories(categoryIds);
    res.json(skus);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post("/forecast", async (req, res) => {
  try {
    const data = await service.getForecastData(req.body);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/weekly-forecast", async (req, res) => {
  try {
    const data = await service.getWeekForecastData(req.body);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/forecastAlerts", async (req, res) => {
  try {
    const data = await service.getForecastAlertData(req.body);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Internal server error" });
  }
});

// Consensus forecast update
router.put("/forecast/consensus", async (req, res) => {
  try {
    const result = await service.updateConsensusForecast(req.body);
    res.status(200).json({
      success: true,
      message: result.message,
      updatedCount: result.updatedCount,
    });
  } catch (error) {
    if (error.message.startsWith("Missing required parameter")) {
      res.status(400).json({
        success: false,
        message: error.message,
      });
    } else {
      console.error("Database update error:", error);
      res.status(500).json({
        success: false,
        message: error.message || "Internal server error",
      });
    }
  }
});

// Generate data for both countries
// In routes, replace the generate/all route section:
// router.post("/generate/all", async (req, res) => {
//   try {
//     console.log("Starting data generation for both countries...");

//     const DataGenerationService = require("../service/dataGenerationService");
//     const dataServiceInstance = new DataGenerationService();

//     console.log("Clearing all existing data...");
//     const totalCleared = await dataServiceInstance.clearTableData();
//     console.log(`Cleared ${totalCleared} existing records`);

//     const results = [];

//     // Generate India data
//     try {
//       const indiaResult = await dataServiceInstance.generateData("India");
//       results.push({
//         country: "India",
//         success: true,
//         recordsGenerated: indiaResult.recordsCount,
//         productsProcessed: indiaResult.productsCount,
//         message: indiaResult.message,
//       });
//     } catch (error) {
//       results.push({
//         country: "India",
//         success: false,
//         error: error.message,
//       });
//     }

//     // Generate USA data
//     try {
//       const usaResult = await dataServiceInstance.generateData("USA");
//       results.push({
//         country: "USA",
//         success: true,
//         recordsGenerated: usaResult.recordsCount,
//         productsProcessed: usaResult.productsCount,
//         message: usaResult.message,
//       });
//     } catch (error) {
//       results.push({
//         country: "USA",
//         success: false,
//         error: error.message,
//       });
//     }

//     const allSuccessful = results.every((r) => r.success);
//     const totalRecords = results.reduce(
//       (sum, r) => sum + (r.recordsGenerated || 0),
//       0
//     );

//     res.status(allSuccessful ? 200 : 207).json({
//       success: allSuccessful,
//       message: allSuccessful
//         ? "Successfully cleared existing data and generated fresh data for both countries"
//         : "Data generation completed with some errors",
//       data: {
//         totalRecords,
//         clearedRecords: totalCleared,
//         results,
//         timestamp: new Date().toISOString(),
//       },
//     });
//   } catch (error) {
//     console.error("Error in bulk data generation:", error);
//     res.status(500).json({
//       success: false,
//       message: "Failed to generate data",
//       error: error.message,
//     });
//   }
// });



// Generate data for both countries
// In routes, replace the generate/all route section:
router.post("/generate/all", async (req, res) => {
  try {
    console.log("Starting data generation for both countries...");

    const DataGenerationService = require("../service/weeklyDataGenerationService");
    const dataServiceInstance = new DataGenerationService();

    console.log("Clearing all existing data...");
    const totalCleared = await dataServiceInstance.clearTableData();
    console.log(`Cleared ${totalCleared} existing records`);

    const results = [];

    // Generate India data
    try {
      const indiaResult = await dataServiceInstance.generateData("India");
      results.push({
        country: "India",
        success: true,
        recordsGenerated: indiaResult.recordsCount,
        productsProcessed: indiaResult.productsCount,
        message: indiaResult.message,
      });
    } catch (error) {
      results.push({
        country: "India",
        success: false,
        error: error.message,
      });
    }

    // Generate USA data
    try {
      const usaResult = await dataServiceInstance.generateData("USA");
      results.push({
        country: "USA",
        success: true,
        recordsGenerated: usaResult.recordsCount,
        productsProcessed: usaResult.productsCount,
        message: usaResult.message,
      });
    } catch (error) {
      results.push({
        country: "USA",
        success: false,
        error: error.message,
      });
    }

    const allSuccessful = results.every((r) => r.success);
    const totalRecords = results.reduce(
      (sum, r) => sum + (r.recordsGenerated || 0),
      0
    );

    res.status(allSuccessful ? 200 : 207).json({
      success: allSuccessful,
      message: allSuccessful
        ? "Successfully cleared existing data and generated fresh data for both countries"
        : "Data generation completed with some errors",
      data: {
        totalRecords,
        clearedRecords: totalCleared,
        results,
        timestamp: new Date().toISOString(),
      },
    });
  } catch (error) {
    console.error("Error in bulk data generation:", error);
    res.status(500).json({
      success: false,
      message: "Failed to generate data",
      error: error.message,
    });
  }
});

module.exports = router;
