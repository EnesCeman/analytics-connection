import pg from "pg";
const { Client } = pg;
import crypto from "crypto";

import dotenv from "dotenv";
dotenv.config();

export const client = new Client(JSON.parse(process.env.PG_CLIENT));

let pg_client;

function generateInsertQuery(dataItem) {
  const columns = Object.keys(dataItem);
  const placeholders = columns.map((_, index) => `$${index + 1}`);
  return {
    columns: columns.join(", "),
    placeholders: placeholders.join(", "),
  };
}

export async function populatePG(pgClient, data, dimensions) {
  try {
    console.log("Inserting or updating data in PG...");

    pgClient.connect();
    const firstDataItem = data[0];

    const concatenatedValues = dimensions
      .map((column) => firstDataItem[column])
      .join("");
    // console.log("concateValues", concatenatedValues);
    const uniqueID = crypto
      .createHash("sha256")
      .update(concatenatedValues)
      .digest("hex");

    const newDataItem = {
      uniqueID,
      ...firstDataItem,
    };

    const { columns, placeholders } = generateInsertQuery(newDataItem);

    const columnsWithTypes = Object.keys(newDataItem).map((columnName) => {
      let dataType = null;
      if (columnName === "date") {
        dataType = "DATE";
      } else if (columnName === "uniqueID") {
        dataType = "TEXT PRIMARY KEY";
      } else if (dimensions.includes(columnName)) {
        dataType = "TEXT";
      } else {
        dataType = "NUMERIC";
      }
      return `${columnName} ${dataType}`;
    });

    // console.log(columnsWithTypes);

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS analyticstopg (
        ${columnsWithTypes.join(", ")}
      )
    `;

    await pgClient.query(createTableQuery);

    for (const item of data) {
      const concatenatedValues = dimensions
        .map((column) => item[column])
        .join("");
      const uniqueID = crypto
        .createHash("sha256")
        .update(concatenatedValues)
        .digest("hex");

      const newDataItem = {
        uniqueID,
        ...item,
      };

      const values = Object.values(newDataItem);

      const query = `INSERT INTO analyticstopg (${columns})
      VALUES (${placeholders})
      ON CONFLICT (uniqueID) DO UPDATE SET
        ${Object.keys(newDataItem)
          .map((col, index) => `${col} = EXCLUDED.${col}`)
          .join(", ")}`;

      await pgClient.query(query, values);
    }
    console.log("Step 2: Data insertion or update in PG... Completed.");
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await pgClient.end();
  }
}

export const exportPGToCSV = async (pgClient) => {
  try {
    pgClient = new Client(JSON.parse(process.env.PG_CLIENT));
    pgClient.connect();

    const query = `SELECT *, to_char(date, 'YYYY-MM-DD') as formatted_date from analyticstopg`;

    const result = await pgClient.query(query);

    const headers = Object.keys(result.rows[0]);
    const rows = result.rows;

    console.log("exportpgtocsv...");
    return { headers, rows };
  } catch (e) {
    console.error("Error", e);
  } finally {
    // Close the database connection
    await pgClient.end();
  }
};
