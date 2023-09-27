import fs from "fs";

import dotenv from "dotenv";
dotenv.config();

export const fromPGToCSVToCloudStorage = async (exp, storage) => {
  try {
    const csvHeaders = [exp.headers.join(",")];

    let csvRows = exp.rows.map((row) => {
      return Object.values(row).map((value) => {
        const regex = /,/g;
        return typeof value === "string" ? value.replace(regex, ";") : value;
      });
    });

    csvRows = csvRows.map((row) => row.join(","));

    const csvData = csvHeaders.concat(csvRows);
    const csvString = csvData.join("\n");
    fs.writeFileSync("analyticstopg.csv", csvString);
    await storage
      .bucket(process.env.STORAGE_BUCKET)
      .upload("analyticstopg.csv", {
        destination: "GA4/analyticstopg.csv",
      });
    console.log(
      "Step 3: File export from PG, creation of CSV and Cloud Storage... Completed."
    );
  } catch (e) {
    console.error("Error: ", e);
  }
};

export async function createDataset(bigquery) {
  try {
    const datasetId = process.env.DATASET_ID;

    const [datasetExists] = await bigquery.dataset(datasetId).exists();

    if (datasetExists) {
      const [tableExists] = await bigquery.dataset(datasetId).getTables();

      if (tableExists) {
        for (const table of tableExists) {
          await table.delete();
        }
      }

      await bigquery.dataset(datasetId).delete();
    }

    await bigquery.createDataset(datasetId);
    console.log(`Dataset ${datasetId} created in BigQuery.`);
  } catch (e) {
    console.log(e);
  }
}

export async function createTable(bigquery) {
  try {
    const datasetId = process.env.DATASET_ID;
    const tableId = process.env.TABLE_ID;

    const [tableExists] = await bigquery
      .dataset(datasetId)
      .table(tableId)
      .exists();

    // console.log(tableExists);

    if (tableExists) {
      await bigquery.dataset(datasetId).table(tableId).delete();
      console.log("existed. deleted.");
    }

    const [table] = await bigquery.dataset(datasetId).createTable(tableId);

    console.log(`Table ${table.id} is created.`);
  } catch (e) {
    console.error(e);
  }
}

export async function loadDataIntoBigQuery(bigquery, storage) {
  try {
    console.log("Loading data into Bigquery table...");
    const metadata = {
      autodetect: true,
      writeDisposition: "WRITE_TRUNCATE",
    };

    await bigquery
      .dataset(process.env.DATASET_ID)
      ?.table(process.env.TABLE_ID)
      ?.load(
        storage
          .bucket(process.env.STORAGE_BUCKET)
          .file("GA4/analyticstopg.csv"),
        metadata
      );

    console.log("Data loaded into BigQuery table.");
  } catch (e) {
    console.error("error?", e);
  }
}
