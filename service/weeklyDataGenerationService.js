const dayjs = require("dayjs");
const weekOfYear = require("dayjs/plugin/weekOfYear.js");
const isoWeek = require("dayjs/plugin/isoWeek.js");
const isoWeeksInYear = require("dayjs/plugin/isoWeeksInYear.js");
const pg = require("pg");
const fs = require("fs");

dayjs.extend(weekOfYear);
dayjs.extend(isoWeek);
dayjs.extend(isoWeeksInYear);

const { Client } = pg;

class WeeklyDataGenerationService {
  constructor() {
    this.today = dayjs();
    this.channels = ["GT", "MT"];
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

  // Weekly-specific utility functions
  getWeekPositionInMonth(weekStartDate, weekEndDate) {
    const midWeekDate = weekStartDate.add(3, "day"); // Wednesday of the week
    const startOfMonth = midWeekDate.startOf("month");
    const weeksDiff = Math.floor(midWeekDate.diff(startOfMonth, "week", true));
    
    return Math.max(1, Math.min(6, Math.ceil(weeksDiff) + 1));
  }

  getWeeklyWeightage(weekPosition, totalWeeksInMonth) {
    // First and last weeks get higher weightage
    if (weekPosition === 1) {
      return this.getRandomBetweenOneAndOnePointFive(1.3, 1.6); // First week
    } else if (weekPosition === totalWeeksInMonth) {
      return this.getRandomBetweenOneAndOnePointFive(1.2, 1.5); // Last week
    } else {
      return this.getRandomBetweenOneAndOnePointFive(0.7, 1.0); // Middle weeks
    }
  }

  getTotalWeeksInMonth(year, month) {
    const startOfMonth = dayjs().year(year).month(month).startOf("month");
    const endOfMonth = dayjs().year(year).month(month).endOf("month");
    
    let weekCount = 0;
    let currentWeek = startOfMonth.startOf("isoWeek");
    
    while (currentWeek.isBefore(endOfMonth.endOf("isoWeek")) || currentWeek.isSame(endOfMonth.endOf("isoWeek"))) {
      const midWeek = currentWeek.add(3, "day"); // Wednesday
      if (midWeek.month() === month) {
        weekCount++;
      }
      currentWeek = currentWeek.add(1, "week");
      
      // Safety check to prevent infinite loop
      if (weekCount > 6) break;
    }
    
    return Math.max(4, weekCount); // Minimum 4 weeks
  }

  adjustActualForCurrentWeek(actualValue, weekStartDate, weekEndDate) {
    const today = dayjs();
    const currentWeekStart = today.startOf("isoWeek");
    
    // Check if the item week is the current week
    if (weekStartDate.isSame(currentWeekStart, "week")) {
      const dayOfWeek = today.isoWeekday(); // 1 = Monday, 7 = Sunday
      
      switch (dayOfWeek) {
        case 1:
        case 2: // Monday, Tuesday
          return Math.round(actualValue * 0.3);
        case 3:
        case 4: // Wednesday, Thursday
          return Math.round(actualValue * 0.6);
        case 5:
        case 6: // Friday, Saturday
          return Math.round(actualValue * 0.85);
        case 7: // Sunday
          return actualValue; // Full week completed
        default:
          return actualValue;
      }
    }
    
    return actualValue; // Return original value if not current week
  }

  generateAdvancedBaseline(product, actual, itemDate, weeksFromNow, config) {
    const monthIndex = dayjs(itemDate).month();

    // 1. Category-specific bias for both India and USA
    const categoryMultipliers = {
      // India categories
      "Sweet Mixes": this.getRandomBetweenOneAndOnePointFive(0.8, 1.2),
      "Beverages": this.getRandomBetweenOneAndOnePointFive(1.1, 1.6),
      "Masala": this.getRandomBetweenOneAndOnePointFive(0.9, 1.3),
      "Ready To Eat": this.getRandomBetweenOneAndOnePointFive(1.0, 1.5),
      // USA categories
      "Breakfast Cereals": this.getRandomBetweenOneAndOnePointFive(1.0, 1.4),
      "Condiments & Sauces": this.getRandomBetweenOneAndOnePointFive(0.9, 1.3),
      "Dairy & Alternatives": this.getRandomBetweenOneAndOnePointFive(1.1, 1.5),
      "Seafood": this.getRandomBetweenOneAndOnePointFive(0.8, 1.2),
    };

    const categoryMultiplier = categoryMultipliers[product.category_name] || 
                              this.getRandomBetweenOneAndOnePointFive(0.9, 1.3);

    // 2. Seasonal baseline bias (different from actual seasonality)
    let seasonalBias = 1;
    if ([1, 2, 10, 11].includes(monthIndex)) {
      // Feb, Mar, Nov, Dec - baseline optimistic
      seasonalBias = this.getRandomBetweenOneAndOnePointFive(1.2, 1.7);
    } else if ([5, 6, 7].includes(monthIndex)) {
      // Jun, Jul, Aug - baseline conservative
      seasonalBias = this.getRandomBetweenOneAndOnePointFive(0.6, 0.9);
    }

    // 3. Future uncertainty (converted from weeks to months)
    const monthsFromNow = Math.floor(weeksFromNow / 4.33);
    const futureUncertainty = monthsFromNow > 0
      ? this.getRandomBetweenOneAndOnePointFive(0.7, 1.4)
      : this.getRandomBetweenOneAndOnePointFive(0.85, 1.15);

    // 4. Weekly volatility (higher than monthly)
    const weeklyVolatility = this.getRandomBetweenOneAndOnePointFive(0.6, 1.4);

    // 5. Random baseline-specific noise
    const baselineNoise = this.getRandomBetweenOneAndOnePointFive(0.7, 1.3);

    const result = Math.round(
      actual *
        categoryMultiplier *
        seasonalBias *
        futureUncertainty *
        weeklyVolatility *
        baselineNoise
    );

    // Ensure we never return NaN or invalid values
    if (isNaN(result) || !isFinite(result)) {
      console.warn(`Invalid baseline calculated for ${product.category_name}, using fallback`);
      return Math.round(actual * this.getRandomBetweenOneAndOnePointFive(0.8, 1.2));
    }

    return result;
  }

  getSeasonalRange({
    state,
    category,
    plat,
    product_name,
    item_date,
    weekPosition,
    totalWeeksInMonth,
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
      January: 0, February: 1, March: 2, April: 3, May: 4, June: 5,
      July: 6, August: 7, September: 8, October: 9, November: 10, December: 11,
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
    }).format(dateObj);

    const monthIndex = dateObj.getMonth();
    const { min, max, trend_peaks, dips } = config;

    // Calculate weekly base values (divide monthly by average weeks per month)
    const avgWeeksPerMonth = 4.33; // 52 weeks / 12 months
    let weeklyMin = Math.floor(min / avgWeeksPerMonth);
    let weeklyMax = Math.floor(max / avgWeeksPerMonth);

    let adjustedMin = weeklyMin;
    let adjustedMax = weeklyMax;
    let adjustedFinalNbr = weeklyMin;
    let mathrndm = Math.random();

    if (trend_peaks.includes(monthName)) {
      adjustedMax = Math.ceil(weeklyMax) * this.getRandomBetweenOneAndOnePointFive(1.5, 2);
      adjustedFinalNbr = Math.round(adjustedMax);
    } else if (dips.includes(monthName)) {
      adjustedMin = Math.floor(weeklyMin) * mathrndm;
      adjustedFinalNbr = Math.round(adjustedMin);
    } else if (
      peakIndexes.some((idx) => idx - 1 === monthIndex || idx - 2 === monthIndex)
    ) {
      adjustedFinalNbr = Math.round(weeklyMax * this.getRandomBetweenOneAndOnePointFive(0.9, 1.1));
    } else if (
      dipIndexes.some((idx) => idx - 1 === monthIndex || idx - 2 === monthIndex)
    ) {
      adjustedFinalNbr = Math.round(weeklyMax * this.getRandomBetweenOneAndOnePointFive(0.6, 0.8));
    } else {
      adjustedFinalNbr = Math.round(
        this.getRandomIntInclusive(weeklyMin, weeklyMax) * this.getRandomBetweenOneAndOnePointFive(0.5, 1)
      );
    }

    // Apply weekly weightage based on position in month
    const weeklyWeightage = this.getWeeklyWeightage(weekPosition, totalWeeksInMonth);
    adjustedFinalNbr = Math.round(adjustedFinalNbr * weeklyWeightage);

    return Math.max(adjustedFinalNbr, 1); // Ensure minimum value of 1
  }

  generateISOWeeks(startDate, endDate) {
    const weeks = [];
    let currentWeek = startDate.startOf("isoWeek");
    
    // Use explicit comparison instead of isSameOrBefore
    while (currentWeek.isBefore(endDate) || currentWeek.isSame(endDate, "day")) {
      const weekStart = currentWeek;
      const weekEnd = currentWeek.endOf("isoWeek");
      const isoYear = weekStart.isoWeekYear();
      const isoWeekNumber = weekStart.isoWeek();
      
      weeks.push({
        weekStart,
        weekEnd,
        isoYear,
        isoWeekNumber,
        weekName: `${isoYear}-W${String(isoWeekNumber).padStart(2, "0")}`,
        monthName: weekStart.add(3, "day").format("MMMM YYYY"), // Use Wednesday for month
      });
      
      currentWeek = currentWeek.add(1, "week");
      
      // Safety check to prevent infinite loops
      if (weeks.length > 500) {
        console.log("Breaking loop - too many weeks generated");
        break;
      }
    }
    
    return weeks;
  }

  generateProducts(config) {
    const products = [];
    const { country, states, cities, cityToPlant, categories } = config;

    for (const state of states) {
      for (const city of cities[state]) {
        const plants = cityToPlant[city] || [];

        for (const plant of plants) {
          for (const [category, skus] of Object.entries(categories)) {
            for (const sku of skus) {
              for (const channel of this.channels) {
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
      let query = "DELETE FROM weekly_demand_forecast";
      let params = [];

      if (country) {
        query += " WHERE country_name = $1";
        params = [country];
      }

      const result = await client.query(query, params);
      console.log(
        `Cleared ${result.rowCount} existing records${
          country ? ` for ${country}` : ""
        }`
      );

      return result.rowCount;
    } finally {
      await client.end();
    }
  }

  async truncateTable() {
    const client = this.createDbClient();
    await client.connect();

    try {
      await client.query("TRUNCATE TABLE weekly_demand_forecast RESTART IDENTITY CASCADE");
      console.log("Successfully truncated weekly_demand_forecast table");
      return true;
    } finally {
      await client.end();
    }
  }

  async generateRecordsForProduct(product, config, seasonalityConfig) {
    const data = [];
    const models = this.getModels();
    const mayActualsByYear = {};

    // Generate weeks from 36 months ago to 18 months in the future
    const startDate = this.today.add(-36, "month").startOf("month");
    const endDate = this.today.add(18, "month").endOf("month");
    const allWeeks = this.generateISOWeeks(startDate, endDate);

    for (const weekInfo of allWeeks) {
      const { weekStart, weekEnd, isoYear, isoWeekNumber, weekName, monthName } = weekInfo;
      
      // Calculate week position in month and total weeks for that month
      const midWeekDate = weekStart.add(3, "day"); // Wednesday
      const weekPosition = this.getWeekPositionInMonth(weekStart, weekEnd);
      const totalWeeksInMonth = this.getTotalWeeksInMonth(midWeekDate.year(), midWeekDate.month());
      
      // Determine if this is past, current, or future
      const isCurrentWeek = weekStart.isSame(this.today.startOf("isoWeek"), "week");
      const isPastWeek = weekEnd.isBefore(this.today);
      const isFutureWeek = weekStart.isAfter(this.today);

      // Generate weekly actual
      let actual = this.getSeasonalRange({
        state: product.state_name,
        category: product.category_name,
        plat: product.plant_name,
        product_name: product.product_name,
        item_date: midWeekDate.format("YYYY-MM-DD"),
        weekPosition,
        totalWeeksInMonth,
        seasonalityConfig,
        defaultConfig: config.defaultConfig,
      });

      // Handle seasonal relationships (October > May)
      const month = midWeekDate.month();
      const year = midWeekDate.year();
      
      if (month === 9 && mayActualsByYear[year] != null) {
        actual = Math.round(Math.max(actual, mayActualsByYear[year] * 0.3));
      }
      
      // Store May weekly actuals
      if (month === 4) {
        if (!mayActualsByYear[year]) mayActualsByYear[year] = 0;
        mayActualsByYear[year] += actual;
      }

      // Generate other metrics
      let onHandUnits = Math.round(actual * this.getRandomBetweenOneAndOnePointFive(0.8, 1.2));

      // Calculate weeks from now for baseline uncertainty
      const weeksFromNow = weekStart.diff(this.today, "week");
      
      let baseline = this.generateAdvancedBaseline(product, actual, weekEnd.format("YYYY-MM-DD"), weeksFromNow, config);

      let consensus = Math.round(actual * this.getRandomBetweenOneAndOnePointFive(0.7, 1.4));
      let levelPct = Math.round(consensus / this.getRandomBetweenOneAndOnePointFive(1.4, 2));
      let stockOutDays = Math.round(this.getRandomIntInclusive(1, 3)); // Weekly stock out days

      let actual_percent = Math.round(this.getRandomIntInclusive(40, 50));
      let ml_forecast_percent = Math.round(this.getRandomBetweenOneAndOnePointFive(30, 40));
      let marketing_percent = 100 - actual_percent - ml_forecast_percent;

      // Apply current week adjustment
      if (isCurrentWeek) {
        actual = this.adjustActualForCurrentWeek(actual, weekStart, weekEnd);
      }

      let revenue = (actual / 1000) * this.getRandomBetweenOneAndOnePointFive(1.1, 1.4);

      for (const model of models) {
        let mape = this.getRandomIntInclusive(10, 100);
        if (model.name === "XGBoost") {
          mape = this.getRandomIntInclusive(0, 40);
        }

        // Festive season adjustments (weekly)
        let promotion = 0;
        if ([9, 10, 11, 3, 4].includes(month)) {
          promotion = Math.round((Math.random() * 100) + 50); // Weekly promotion
        }

        let ml = Math.round(actual * this.getRandomBetweenOneAndOnePointFive(0.9, 1.1));

        data.push({
          country_name: product.country_name,
          state_name: product.state_name,
          city_name: product.city_name,
          plant_name: product.plant_name,
          category_name: product.category_name,
          sku_code: product.sku_code,
          actual_units: isPastWeek || isCurrentWeek ? Math.round(actual) : null,
          product_name: product.product_name,
          channel_name: product.channel_name,
          item_date: weekEnd.format("YYYY-MM-DD"), // End of week as reference
          week_start_date: weekStart.format("YYYY-MM-DD"),
          week_end_date: weekEnd.format("YYYY-MM-DD"),
          baseline_forecast: Math.round(baseline),
          ml_forecast: Math.round(ml),
          sales_units: isPastWeek || isCurrentWeek ? Math.round(ml * 0.8) : null,
          promotion_marketing: isPastWeek || isCurrentWeek ? Math.round(ml * 0.2) : null,
          revenue_forecast_lakhs: revenue.toFixed(2),
          model_name: model.name,
          month_name: monthName,
          week_name: weekName,
          iso_week_number: isoWeekNumber,
          iso_year: isoYear,
          week_position_in_month: weekPosition,
          inventory_level_pct: isCurrentWeek ? levelPct : null,
          stock_out_days: isCurrentWeek ? stockOutDays : null,
          on_hand_units: isCurrentWeek ? onHandUnits : null,
          consensus_forecast: !isFutureWeek ? consensus : null,
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
          `INSERT INTO weekly_demand_forecast
          (country_name, state_name, city_name, plant_name, category_name, sku_code, channel_name, product_name,
           actual_units, baseline_forecast, ml_forecast, sales_units, promotion_marketing, consensus_forecast,
           revenue_forecast_lakhs, inventory_level_pct, stock_out_days, on_hand_units, item_date, 
           week_start_date, week_end_date, month_name, week_name, iso_week_number, iso_year, 
           week_position_in_month, model_name, mape, marketing_percent, ml_forecast_percent, actual_percent)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31)`,
          [
            row.country_name, row.state_name, row.city_name, row.plant_name,
            row.category_name, row.sku_code, row.channel_name, row.product_name,
            row.actual_units, row.baseline_forecast, row.ml_forecast, row.sales_units,
            row.promotion_marketing, row.consensus_forecast, row.revenue_forecast_lakhs,
            row.inventory_level_pct, row.stock_out_days, row.on_hand_units, row.item_date,
            row.week_start_date, row.week_end_date, row.month_name, row.week_name,
            row.iso_week_number, row.iso_year, row.week_position_in_month, row.model_name,
            row.mape, row.marketing_percent, row.ml_forecast_percent, row.actual_percent
          ]
        );
      }
    } finally {
      await client.end();
    }
  }

  async getTableStats() {
    const client = this.createDbClient();
    await client.connect();

    try {
      const result = await client.query(`
        SELECT COUNT(*) as total_records,
               COUNT(CASE WHEN country_name = 'India' THEN 1 END) as india_records,
               COUNT(CASE WHEN country_name = 'USA' THEN 1 END) as usa_records,
               MIN(item_date) as earliest_date,
               MAX(item_date) as latest_date
        FROM weekly_demand_forecast
      `);
      
      return result.rows[0];
    } finally {
      await client.end();
    }
  }

  async generateData(country) {
    try {
      const config = country === "USA" ? this.getUSAConfig() : this.getIndiaConfig();

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
      console.log(`Generated ${products.length} product combinations for ${country}`);

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
          console.error(`Error generating records for product ${product.product_name}:`, error);
        }
      }

      // Insert into database
      await this.insertData(allData);
      
      return {
        success: true,
        message: `Successfully generated and inserted ${allData.length} weekly records for ${country}`,
        recordsCount: allData.length,
        productsCount: products.length,
      };
    } catch (error) {
      throw new Error(`Failed to generate weekly data for ${country}: ${error.message}`);
    }
  }

  async generateAllData() {
    const startTime = Date.now();
    const logs = [];

    try {
      logs.push({ 
        step: 'start', 
        message: 'Starting complete weekly data generation workflow', 
        timestamp: new Date().toISOString() 
      });

      // Step 1: Truncate table
      console.log('Step 1: Truncating weekly_demand_forecast table...');
      logs.push({ 
        step: 'truncate_start', 
        message: 'Truncating table', 
        timestamp: new Date().toISOString() 
      });
      
      await this.truncateTable();
      logs.push({ 
        step: 'truncate_complete', 
        message: 'Table truncated successfully', 
        timestamp: new Date().toISOString() 
      });

      // Step 2: Generate India data
      console.log('Step 2: Generating India weekly data...');
      logs.push({ 
        step: 'india_start', 
        message: 'Starting India data generation', 
        timestamp: new Date().toISOString() 
      });
      
      const indiaResult = await this.generateData('India');
      logs.push({ 
        step: 'india_complete', 
        message: `India data generation completed: ${indiaResult.recordsCount} records`, 
        timestamp: new Date().toISOString() 
      });

      // Step 3: Generate USA data
      console.log('Step 3: Generating USA weekly data...');
      logs.push({ 
        step: 'usa_start', 
        message: 'Starting USA data generation', 
        timestamp: new Date().toISOString() 
      });
      
      const usaResult = await this.generateData('USA');
      logs.push({ 
        step: 'usa_complete', 
        message: `USA data generation completed: ${usaResult.recordsCount} records`, 
        timestamp: new Date().toISOString() 
      });

      // Step 4: Get final stats
      const finalStats = await this.getTableStats();
      const endTime = Date.now();
      const totalDuration = Math.round((endTime - startTime) / 1000);

      logs.push({ 
        step: 'complete', 
        message: `Complete workflow finished in ${totalDuration} seconds`, 
        timestamp: new Date().toISOString(),
        stats: finalStats
      });

      console.log(`Complete weekly data generation workflow completed in ${totalDuration} seconds`);

      return {
        success: true,
        message: 'Complete weekly data generation workflow completed successfully',
        duration_seconds: totalDuration,
        final_stats: {
          total_records: parseInt(finalStats.total_records),
          india_records: parseInt(finalStats.india_records),
          usa_records: parseInt(finalStats.usa_records),
          earliest_date: finalStats.earliest_date,
          latest_date: finalStats.latest_date
        },
        workflow_logs: logs,
        details: {
          india_result: indiaResult,
          usa_result: usaResult
        },
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      const endTime = Date.now();
      const totalDuration = Math.round((endTime - startTime) / 1000);
      
      logs.push({ 
        step: 'error', 
        message: `Workflow failed: ${error.message}`, 
        timestamp: new Date().toISOString() 
      });

      console.error('Error in complete weekly data generation workflow:', error);
      
      throw {
        success: false,
        message: 'Complete weekly data generation workflow failed',
        duration_seconds: totalDuration,
        error: error.message,
        workflow_logs: logs,
        details: error,
        timestamp: new Date().toISOString()
      };
    }
  }
}

module.exports = WeeklyDataGenerationService;