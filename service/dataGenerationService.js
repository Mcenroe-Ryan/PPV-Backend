const dayjs = require("dayjs");
const weekOfYear = require("dayjs/plugin/weekOfYear.js");
const pg = require("pg");
const fs = require("fs");

dayjs.extend(weekOfYear);

const { Client } = pg;

class DataGenerationService {
  constructor() {
    this.today = dayjs();
  }

  createDbClient() {
    return new Client({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      port: process.env.DB_PORT,
      // ssl: { rejectUnauthorized: false } //for RDS
    });
  }

  getIndiaConfig() {
    return {
      country: "India",
      states: ["Karnataka", "Andhra Pradesh"],
      cities: {
        Karnataka: ["Bengaluru", "Hubballi", "Udupi"],
        "Andhra Pradesh": ["Tirupati", "Vijayawada", "Visakhapatnam"],
      },
      cityToPlant: {
        Bengaluru: ["Kar123"],
        Udupi: ["Kar124"],
        Hubballi: ["Kar125"],
        Tirupati: ["And126"],
        Vijayawada: ["And201"],
        Visakhapatnam: ["And202"],
      },
      categories: {
        "Sweet Mixes": [
          { code: "SKU-GULAB", product_name: "Gulab Jamun - 200gm" },
          { code: "SKU-RASG", product_name: "Coconut Burfi - 100gm" },
        ],
        Beverages: [
          { code: "SKU-BADAM-MILK", product_name: "Badam Milk - 200ml" },
          {
            code: "SKU-CHOCOLATE",
            product_name: "Chocolate Milk Shake - 200ml",
          },
        ],
        Masala: [
          { code: "SKU-GARAM", product_name: "Garam Masala - 100gm" },
          { code: "SKU-CHILLI", product_name: "Chilli Powder - 100gm" },
          { code: "SKU-SAMBHAR", product_name: "Sambhar Powder - 100gm" },
        ],
        "Ready To Eat": [
          { code: "SKU-POHA", product_name: "Instant Poha - 250gm" },
          { code: "SKU-UPMA", product_name: "Rava Upma - 500gm" },
          { code: "SKU-DOSAMIX", product_name: "Dosa Mix - 250gm" },
        ],
      },
      seasonalityFile: "./config/output.json",
      defaultConfig: {
        state: "Karnataka",
        category: "Masala",
        plat: "Kar123",
        product_name: "Sambhar Powder - 100gm",
        min: 2500,
        max: 4000,
        trend_peaks: ["May", "October"],
        dips: [],
      },
    };
  }

  getUSAConfig() {
    return {
      country: "USA",
      states: ["California", "Texas"],
      cities: {
        California: ["Los Angeles", "San Francisco", "San Diego"],
        Texas: ["Houston", "Dallas", "Austin"],
      },
      cityToPlant: {
        "Los Angeles": ["LA123"],
        "San Francisco": ["SF124"],
        "San Diego": ["SD125"],
        Houston: ["HS201"],
        Dallas: ["Da202"],
        Austin: ["Au203"],
      },
      categories: {
        "Breakfast Cereals": [
          { code: "SKU-CORN", product_name: "Cornflakes - 200 gm" },
          { code: "SKU-OATS", product_name: "Oats - 100 gm" },
        ],
        "Condiments & Sauces": [
          { code: "SKU-KETCHUP", product_name: "Ketchup - 200 ml" },
          { code: "SKU-BBQ", product_name: "BBQ Sauce - 200 ml" },
          { code: "SKU-HOTSAUCE", product_name: "Hot Sauce - 100 ml" },
        ],
        "Dairy & Alternatives": [
          { code: "SKU-ALMOND", product_name: "Almond Milk - 200 ml" },
          { code: "SKU-YOGURT", product_name: "Yogurt - 200 ml" },
        ],
        Seafood: [
          { code: "SKU-SHRIMP", product_name: "Frozen Shrimp - 250gm" },
          { code: "SKU-TUNA", product_name: "Canned Tuna - 500gm" },
          { code: "SKU-SALMON", product_name: "Salmon Fillets - 250gm" },
        ],
      },
      seasonalityFile: "./config/output_usa.json",
      defaultConfig: {
        state: "California",
        city: "Los Angeles",
        category: "Breakfast Cereals",
        plant: "LA123",
        product_name: "Cornflakes - 200 gm",
        min: 2500,
        max: 4500,
        trend_peaks: ["Nov", "Dec"],
        dips: ["Feb"],
      },
    };
  }

  getModels() {
    return [
      {
        name: "XGBoost",
        baselineFactor: 1.05,
        mlFactor: 1.07,
        revenueFactor: 1.06,
      },
      {
        name: "LightGBM",
        baselineFactor: 1.02,
        mlFactor: 1.05,
        revenueFactor: 1.04,
      },
      {
        name: "ARIMA",
        baselineFactor: 0.97,
        mlFactor: 0.95,
        revenueFactor: 0.9,
      },
    ];
  }

  getRandomIntInclusive(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  getRandomBetweenOneAndOnePointFive(min, max) {
    return Math.random() * (max - min) + min;
  }

  getCurrentWeekOfMonth() {
    const today = dayjs();
    const startOfMonth = today.startOf("month");
    const currentDay = today.date();
    const weekOfMonth = Math.ceil(currentDay / 7);
    return Math.min(weekOfMonth, 4);
  }

  adjustActualForCurrentWeek(actualValue, itemDate) {
    const today = dayjs();
    const itemDateObj = dayjs(itemDate);

    if (itemDateObj.format("YYYY-MM") === today.format("YYYY-MM")) {
      const currentWeek = this.getCurrentWeekOfMonth();

      switch (currentWeek) {
        case 1:
          return Math.round(actualValue / 4);
        case 2:
          return Math.round(actualValue / 3);
        case 3:
        case 4:
          return actualValue;
        default:
          return actualValue;
      }
    }

    return actualValue;
  }

  generateAdvancedBaseline(product, actual, itemDate, monthsFromNow) {
    const monthIndex = dayjs(itemDate).month();

    // 1. Category-specific bias
    const categoryMultiplier = {
      "Sweet Mixes": getRandomBetweenOneAndOnePointFive(0.8, 1.2),
      Beverages: getRandomBetweenOneAndOnePointFive(1.1, 1.6),
      Masala: getRandomBetweenOneAndOnePointFive(0.9, 1.3),
      "Ready To Eat": getRandomBetweenOneAndOnePointFive(1.0, 1.5),
    }[product.category_name];

    // 2. Seasonal baseline bias (different from actual seasonality)
    let seasonalBias = 1;
    if ([1, 2, 10, 11].includes(monthIndex)) {
      // Feb, Mar, Nov, Dec - baseline optimistic
      seasonalBias = getRandomBetweenOneAndOnePointFive(1.2, 1.7);
    } else if ([5, 6, 7].includes(monthIndex)) {
      // Jun, Jul, Aug - baseline conservative
      seasonalBias = getRandomBetweenOneAndOnePointFive(0.6, 0.9);
    }

    // 3. Future uncertainty
    const futureUncertainty =
      monthsFromNow > 0
        ? getRandomBetweenOneAndOnePointFive(0.7, 1.4)
        : getRandomBetweenOneAndOnePointFive(0.85, 1.15);

    // 4. Random baseline-specific noise
    const baselineNoise = getRandomBetweenOneAndOnePointFive(0.7, 1.3);

    return Math.round(
      actual *
        categoryMultiplier *
        seasonalBias *
        futureUncertainty *
        baselineNoise
    );
  }

  getSeasonalRange({
    state,
    category,
    plat,
    product_name,
    item_date,
    seasonalityConfig,
    defaultConfig,
  }) {
    let config = seasonalityConfig.find(
      (item) =>
        item.state === state &&
        item.category === category &&
        item.plat === plat &&
        item.product_name === product_name
    );

    const monthToIndex = {
      January: 0,
      February: 1,
      March: 2,
      April: 3,
      May: 4,
      June: 5,
      July: 6,
      August: 7,
      September: 8,
      October: 9,
      November: 10,
      December: 11,
    };

    if (!config) {
      config = defaultConfig;
    }

    const peakIndexes = config.trend_peaks?.map((m) => monthToIndex[m]);
    const dipIndexes = config.dips?.map((m) => monthToIndex[m]);

    const dateObj = new Date(item_date);
    if (isNaN(dateObj)) {
      throw new Error(`Invalid date format passed: ${item_date}`);
    }

    const monthName = new Intl.DateTimeFormat("en-US", {
      month: "long",
    }).format(new Date(item_date));

    const monthIndex = dateObj.getMonth();
    const { min, max, trend_peaks, dips } = config;

    let adjustedMin = min;
    let adjustedMax = max;
    let adjustedFinalNbr = min;
    let mathrndm = Math.random();

    if (trend_peaks.includes(monthName)) {
      adjustedMax =
        Math.ceil(max) * this.getRandomBetweenOneAndOnePointFive(1.5, 2);
      adjustedFinalNbr = Math.round(adjustedMax);
    } else if (dips.includes(monthName)) {
      adjustedMin = Math.floor(min) * mathrndm;
      adjustedFinalNbr = Math.round(adjustedMin);
    } else if (
      peakIndexes.some(
        (idx) => idx - 1 === monthIndex || idx - 2 === monthIndex
      )
    ) {
      adjustedFinalNbr = Math.round(
        max * this.getRandomBetweenOneAndOnePointFive(0.9, 1.1)
      );
    } else if (
      dipIndexes.some((idx) => idx - 1 === monthIndex || idx - 2 === monthIndex)
    ) {
      adjustedFinalNbr = Math.round(
        max * this.getRandomBetweenOneAndOnePointFive(0.6, 0.8)
      );
    } else {
      adjustedFinalNbr = Math.round(
        this.getRandomIntInclusive(min, max) *
          this.getRandomBetweenOneAndOnePointFive(0.5, 1)
      );
    }

    return adjustedFinalNbr;
  }

  generateRandomFromSeason({
    state,
    category,
    plat,
    product_name,
    item_date,
    seasonalityConfig,
    defaultConfig,
  }) {
    const adjustedNbr = this.getSeasonalRange({
      state,
      category,
      plat,
      product_name,
      item_date,
      seasonalityConfig,
      defaultConfig,
    });
    return adjustedNbr;
  }

  generateProducts(config) {
    const products = [];
    const { country, states, cities, cityToPlant, categories } = config;
    const channels = ["GT", "MT"];

    for (const state of states) {
      for (const city of cities[state]) {
        const plants = cityToPlant[city] || [];

        for (const plant of plants) {
          for (const [category, skus] of Object.entries(categories)) {
            for (const sku of skus) {
              for (const channel of channels) {
                products.push({
                  country_name: country,
                  state_name: state,
                  city_name: city,
                  plant_name: plant,
                  category_name: category,
                  sku_code: sku.code,
                  product_name: sku.product_name,
                  channel_name: channel,
                });
              }
            }
          }
        }
      }
    }

    return products;
  }
  async clearTableData(country = null) {
    const client = this.createDbClient();
    await client.connect();

    try {
      let query = "DELETE FROM demand_forecast";
      let params = [];

      if (country) {
        query += " WHERE country_name = $1";
        params = [country];
      }

      const result = await client.query(query, params);
      console.log(
        `🗑️  Cleared ${result.rowCount} existing records${
          country ? ` for ${country}` : ""
        }`
      );

      return result.rowCount;
    } finally {
      await client.end();
    }
  }

  async generateRecordsForProduct(product, config, seasonalityConfig) {
    const data = [];
    const models = this.getModels();
    const mayActualsByYear = {};

    for (let i = -36; i <= 18; i++) {
      const itemDate = this.today.add(i, "month").endOf("month");
      const monthName = itemDate.format("MMMM YYYY");
      const weekName = `Week ${itemDate.week()}`;
      const monthIndex = itemDate.month();
      const year = itemDate.year();

      let actual = this.generateRandomFromSeason({
        state: product.state_name,
        category: product.category_name,
        plat: product.plant_name,
        product_name: product.product_name,
        item_date: itemDate.format("YYYY-MM-DD"),
        seasonalityConfig,
        defaultConfig: config.defaultConfig,
      });

      // Ensure October > May for the same year
      if (monthIndex === 9 && mayActualsByYear[year] != null) {
        actual = Math.round(
          Math.max(
            actual,
            mayActualsByYear[year] *
              this.getRandomBetweenOneAndOnePointFive(1, 1.3)
          )
        );
      }
      if (monthIndex === 10 && mayActualsByYear[year] != null) {
        actual = Math.round(
          Math.max(
            actual,
            mayActualsByYear[year] *
              this.getRandomBetweenOneAndOnePointFive(0.6, 0.8)
          )
        );
      }
      if (monthIndex === 11 && mayActualsByYear[year] != null) {
        const multiplier =
          config.country === "USA"
            ? this.getRandomBetweenOneAndOnePointFive(1, 1.3)
            : this.getRandomBetweenOneAndOnePointFive(0.4, 0.6);
        actual = Math.round(
          Math.max(actual, mayActualsByYear[year] * multiplier)
        );
      }

      let onHandUnits = this.generateRandomFromSeason({
        state: product.state_name,
        category: product.category_name,
        plat: product.plant_name,
        product_name: product.product_name,
        item_date: itemDate.format("YYYY-MM-DD"),
        seasonalityConfig,
        defaultConfig: config.defaultConfig,
      });

      // let baseline = Math.round(
      //   actual * this.getRandomBetweenOneAndOnePointFive(0.2, 1.8)
      // );
      let baseline = generateAdvancedBaseline(product, actual, itemDate, i);

      let consensus = Math.round(
        actual * this.getRandomBetweenOneAndOnePointFive(0.7, 1.4)
      );

      // let levelPct = this.getRandomBetweenOneAndOnePointFive(8, 20);
      // let stockOutDays = 2 + (Math.abs(i) % 5);
      let levelPct = Math.round(
        consensus / this.getRandomBetweenOneAndOnePointFive(2, 2.5)
      );
      let stockOutDays = Math.round(this.getRandomIntInclusive(14, 21));

      let actual_percent = Math.round(
        this.getRandomBetweenOneAndOnePointFive(40, 50)
      );
      let ml_forecast_percent = Math.round(
        this.getRandomBetweenOneAndOnePointFive(30, 40)
      );
      let marketing_percent = 100 - actual_percent - ml_forecast_percent;

      // Apply current week adjustment for actual values (only for India)
      if (config.country === "India") {
        actual = this.adjustActualForCurrentWeek(
          actual,
          itemDate.format("YYYY-MM-DD")
        );
      }

      let revenue =
        (actual / 1000) * this.getRandomBetweenOneAndOnePointFive(1.1, 1.4);

      for (const model of models) {
        let mape = this.getRandomIntInclusive(10, 100);
        if (model.name === "XGBoost") {
          mape = this.getRandomIntInclusive(0, 40);
        }

        // Store May actual per year
        if (monthIndex === 4) {
          mayActualsByYear[year] = actual;
        }

        let ml = Math.round(
          actual * this.getRandomBetweenOneAndOnePointFive(0.9, 1.1)
        );

        data.push({
          country_name: product.country_name,
          state_name: product.state_name,
          city_name: product.city_name,
          plant_name: product.plant_name,
          category_name: product.category_name,
          sku_code: product.sku_code,
          actual_units: i <= 0 ? Math.round(actual) : null,
          product_name: product.product_name,
          channel_name: product.channel_name,
          item_date: itemDate.format("YYYY-MM-DD"),
          baseline_forecast: Math.round(baseline),
          ml_forecast: Math.round(ml),
          sales_units: i <= 1 ? Math.round(ml * 0.8) : null,
          promotion_marketing: i <= 1 ? Math.round(ml * 0.2) : null,
          revenue_forecast_lakhs: revenue.toFixed(2),
          model_name: model.name,
          month_name: monthName,
          week_name: weekName,
          inventory_level_pct: i === 0 ? levelPct : null,
          stock_out_days: i === 0 ? stockOutDays : null,
          on_hand_units: i === 0 ? onHandUnits : null,
          consensus_forecast: i <= 1 ? consensus : null,
          mape: mape,
          marketing_percent: marketing_percent,
          ml_forecast_percent: ml_forecast_percent,
          actual_percent: actual_percent,
        });
      }
    }
    return data;
  }

  async insertData(rows) {
    const client = this.createDbClient();
    await client.connect();

    try {
      for (const row of rows) {
        await client.query(
          `INSERT INTO demand_forecast
          (country_name, state_name, city_name, plant_name, category_name, sku_code, channel_name, product_name,
           actual_units, baseline_forecast, ml_forecast, sales_units, promotion_marketing, consensus_forecast,
           revenue_forecast_lakhs, inventory_level_pct, stock_out_days, on_hand_units, item_date, month_name, 
           week_name, model_name, mape, marketing_percent, ml_forecast_percent, actual_percent )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26 )`,
          [
            row.country_name,
            row.state_name,
            row.city_name,
            row.plant_name,
            row.category_name,
            row.sku_code,
            row.channel_name,
            row.product_name,
            row.actual_units,
            row.baseline_forecast,
            row.ml_forecast,
            row.sales_units,
            row.promotion_marketing,
            row.consensus_forecast,
            row.revenue_forecast_lakhs,
            row.inventory_level_pct,
            row.stock_out_days,
            row.on_hand_units,
            row.item_date,
            row.month_name,
            row.week_name,
            row.model_name,
            row.mape,
            row.marketing_percent,
            row.ml_forecast_percent,
            row.actual_percent,
          ]
        );
      }
    } finally {
      await client.end();
    }
  }

  async generateData(country) {
    try {
      const config =
        country === "USA" ? this.getUSAConfig() : this.getIndiaConfig();

      // Load seasonality configuration
      let seasonalityConfig = [];
      try {
        const rawData = fs.readFileSync(config.seasonalityFile, "utf-8");
        seasonalityConfig = JSON.parse(rawData);
      } catch (error) {
        console.warn(
          `Could not load seasonality config from ${config.seasonalityFile}, using defaults`
        );
      }

      // Generate products
      const products = this.generateProducts(config);

      // Generate all data
      const allData = [];
      for (const product of products) {
        try {
          const records = await this.generateRecordsForProduct(
            product,
            config,
            seasonalityConfig
          );
          allData.push(...records);
        } catch (error) {
          console.log(error);
        }
      }
      // Insert into database
      await this.insertData(allData);
      console.log(" after insert");
      return {
        success: true,
        message: `✅ Successfully generated and inserted ${allData.length} records for ${country}`,
        recordsCount: allData.length,
        productsCount: products.length,
      };
    } catch (error) {
      throw new Error(
        `Failed to generate data for ${country}: ${error.message}`
      );
    }
  }
}

module.exports = DataGenerationService;
