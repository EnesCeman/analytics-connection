import { populatePG, exportPGToCSV, client as pgClient } from "./postgres.js";

import { BetaAnalyticsDataClient } from "@google-analytics/data";
import { GoogleAuth } from "google-auth-library";
import {
  createDataset,
  createTable,
  fromPGToCSVToCloudStorage,
  loadDataIntoBigQuery,
} from "./bigquery.js";
import { Storage } from "@google-cloud/storage";
import { BigQuery } from "@google-cloud/bigquery";

import dotenv from "dotenv";
dotenv.config();

import schedule from "node-schedule";

let analyticsFileContent = process.env.ANALYTICS_JSON;
analyticsFileContent = JSON.parse(analyticsFileContent);

let bigqueryFileContent = process.env.BIGQUERY_JSON;
bigqueryFileContent = JSON.parse(bigqueryFileContent);

// // Path to the JSON key file downloaded in step 2
// const keyFilePath = "./emp-data-pipeline-3d4da71f47f8.json";

// Initialize the GoogleAuth client with the service account credentials
const auth = new GoogleAuth({
  credentials: analyticsFileContent,
  scopes: ["https://www.googleapis.com/auth/analytics.readonly"],
});

// Create a client for the GA4 Data API
const analyticsDataClient = new BetaAnalyticsDataClient({
  auth: auth,
});

const storage = new Storage({
  projectId: process.env.PROJECT_ID,
  credentials: bigqueryFileContent,
});

const bigquery = new BigQuery({
  projectId: process.env.PROJECT_ID,
  credentials: bigqueryFileContent,
});

const runReport = async () => {
  try {
    const [response] = await analyticsDataClient.runReport({
      property: "properties/395632331", // Replace with your GA4 property ID
      dateRanges: [
        {
          startDate: "2023-01-01",
          endDate: "yesterday",
        },
      ],
      dimensions: [
        {
          name: "date",
        },
        {
          name: "pagePath",
        },
        {
          name: "pageTitle",
        },
        {
          name: "sessionMedium",
        },
        {
          name: "sessionSource",
        },
        {
          name: "landingPage",
        },
        {
          name: "browser",
        },
        {
          name: "day",
        },
      ],
      metrics: [
        {
          name: "sessions",
        },
        {
          name: "screenPageViews",
        },
        {
          name: "activeUsers",
        },
        {
          name: "userEngagementDuration",
        },
        {
          name: "engagementRate",
        },
        {
          name: "conversions",
        },
        {
          name: "totalRevenue",
        },
        {
          name: "averageSessionDuration",
        },
        {
          name: "bounceRate",
        },
        {
          name: "newUsers",
        },
      ],
    });

    const dimensions = response.dimensionHeaders.map((item) => item.name);

    const transformedData = [];

    response.rows.forEach((row) => {
      const transformedRow = {};

      response.dimensionHeaders.forEach((header, index) => {
        transformedRow[header.name] = row.dimensionValues[index].value;
      });

      response.metricHeaders.forEach((header, index) => {
        transformedRow[header.name] = row.metricValues[index].value;
      });

      transformedData.push(transformedRow);
    });

    console.log("Step 1: Pulling data from GA4... Completed.");
    return { transformedData, dimensions };
  } catch (e) {
    console.error("Error:", e);
  }
};

const job = schedule.scheduleJob("48 * * * *", () => {
  (async () => {
    try {
      console.log("starting process at " + new Date());
      const { transformedData, dimensions } = await runReport();
      await populatePG(pgClient, transformedData, dimensions);
      const exp = await exportPGToCSV(pgClient);
      await fromPGToCSVToCloudStorage(exp, storage);
      await createDataset(bigquery);
      await createTable(bigquery);
      await loadDataIntoBigQuery(bigquery, storage);
    } catch (e) {
      console.error("inital error", e);
    }
  })();
});
