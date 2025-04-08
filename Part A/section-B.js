// -------------------Import libraries-------------------------------
const fs = require('fs');
const fastCsv = require('fast-csv');
const Csv = require('csv-parser');
const XLSX = require('xlsx');
const moment = require('moment');
const csv = require('csv-parse/sync');

// ------------------Convert Excel file to CSV-----------------------

// const excelFilePath = 'time_series.xlsx';  // Excel file name
// const outputCsvPath = 'File.csv';  // CSV file name

// function convertExcelTocsv() {
// //Read Excel file
// const workbook = XLSX.readFile(excelFilePath);
// const sheetName = workbook.SheetNames[0]; // Select the first sheet
// const sheet = workbook.Sheets[sheetName];

// // Convert sheet to CSV format
// const csvData = XLSX.utils.sheet_to_csv(sheet);

// // Write data to file
// fs.writeFileSync(outputCsvPath, csvData, 'utf8');
// }

// ------------------------Define variables-----------------------------
const filepath = 'time_series.csv';
const filepathPARQUET = 'time_series.parquet';
const rowsSet = new Set();
const invalidTimestamps = [];
const duplicateRows = [];
const missingValues = [];
const invalidValues = [];
const hourlyData = {}; 
const outputFolder = 'daily_chunks';  



//------------------------Section B-1-----------------------------

//  -----------Data Validation function-------------------------
function validateData(row) {
 
  const timestamp = row.timestamp?.trim();
  const value = row.value?.trim();


 // Check for valid timestamp format
  if (!moment(timestamp, 'DD/MM/YYYY HH:mm', true).isValid()) {
      invalidTimestamps.push(timestamp);
      return false;
  }
  
  // Check for duplicates
  if (rowsSet.has(row)) {
    duplicateRows.push(row);
    return false;
  } else {
    rowsSet.add(row);
  }

  // Check for missing values
  if (!timestamp || !value) {
    missingValues.push(row);
    return false;
  }

  // Check for numeric values
  if (isNaN(parseFloat(value))) {
    invalidValues.push(value);
    return false;
  }
  return true;
}

// ------------Function to calculate hourly averages with output--------------
function calculateHourlyAverages() {
  fs.createReadStream(filepath)
    .pipe(Csv())
    .on('data', (row) => {
       // Validation
      if (!validateData(row)) return;
    
      const timestamp = row.timestamp;
      const value = parseFloat(row.value);

      if (!isNaN(value)) {
        // Round the timestamp to the nearest hour (YYYY-MM-DD HH:00:00)
        const hourKey = moment(timestamp, 'DD-MM-YYYY HH:mm').startOf('hour').format('DD-MM-YYYY HH:mm');
        
        if (!hourlyData[hourKey]) {
          hourlyData[hourKey] = { sum: 0, count: 0 };
        }
        hourlyData[hourKey].sum += value;
        hourlyData[hourKey].count += 1;
      }
    })
    .on('end', () => {
      console.log(' Average values per hour:');
      Object.keys(hourlyData).sort().forEach(hour => {
        const avg = (hourlyData[hour].sum / hourlyData[hour].count).toFixed(2);
        console.log(`${hour} - ${avg}`);
      });
    });
}


//------------------------Section B-2-----------------------------

// -----------------Function to process daily chunks without stream--------------------
function processDailyChunksNoStream() {
    // Create folder if it doesn't exist
    if (!fs.existsSync(outputFolder)) {
      fs.mkdirSync(outputFolder);
    }
  
    const dailyFiles = {}; 
    
    //  Read the entire CSV file
    const fileContent = fs.readFileSync(filepath, 'utf8');
    const records = csv.parse(fileContent, { columns: true });
  
    // Split the data by day
    records.forEach(row => {
         // Validation
       if (!validateData(row)) return;
      const timestamp = row.timestamp;
      const value = parseFloat(row.value);
  
      if (!isNaN(value)) {
        const dayKey = moment(timestamp, 'DD-MM-YYYY HH:mm').format('DD-MM-YYYY');
  
        if (!dailyFiles[dayKey]) {
          dailyFiles[dayKey] = [];
        }
  
        dailyFiles[dayKey].push({ timestamp, value });
      }
    });
  
    // Write each day to a separate file
    Object.keys(dailyFiles).forEach(day => {
      const filePath = `${outputFolder}/${day}.csv`;
      const csvData = 'timestamp,value\n' + dailyFiles[day]
        .map(entry => `${entry.timestamp},${entry.value}`)
        .join('\n');
  
      fs.writeFileSync(filePath, csvData, 'utf8');
      console.log(` File created: ${filePath}`);
    });
  
    hourlyAverageNoStream();
  
    // ---------------------Function to Calculate hourly averages for each daily file without stream--------------------
    function hourlyAverageNoStream() {
      const allResults = [];
      const files = fs.readdirSync(outputFolder);
  
      files.forEach(file => {
        const filePath = `${outputFolder}/${file}`;
        const fileContent = fs.readFileSync(filePath, 'utf8');
        const records = csv.parse(fileContent, { columns: true });
        
        const hourlyData = {};
        records.forEach(row => {
             // Validation
      if (!validateData(row)) return;
          const timestamp = row.timestamp;
          const value = parseFloat(row.value);
  
          if (!isNaN(value)) {
            const hourKey = moment(timestamp, 'DD-MM-YYYY HH:mm').startOf('hour').format('DD-MM-YYYY HH:mm');
            
            if (!hourlyData[hourKey]) {
              hourlyData[hourKey] = { sum: 0, count: 0 };
            }
            hourlyData[hourKey].sum += value;
            hourlyData[hourKey].count += 1;
          }
        });
  
        Object.keys(hourlyData).forEach(hour => {
          const avg = (hourlyData[hour].sum / hourlyData[hour].count).toFixed(2);
          allResults.push({ timestamp: hour, average: avg });
        });
  
        console.log(` Averages calculation complete for ${file}`);
      });
  
      writeFinalResults(allResults);
    }
  }




//---------------------------Section B-3----------------------------------

//כאשר הנתונים זורמים במקום להיקרא מקובץ שלם, יש לעבד כל רשומה באופן מידי בזמן אמת
//לחלק את הקובץ לימים ולחשב את הממוצע השעתי 
//תוך כדי זרימה במקום להמתין לקריאה מלאה של כל הנתונים
//בשונה מסעיף הקודם שבו קודם נטען כל הקובץ לזיכרון ורק אז נעשו הפעולות


// --------------Function to process daily chunks with stream------------------------
function processDailyChunksWithStream() {
  if (!fs.existsSync(outputFolder)) {
    fs.mkdirSync(outputFolder);
  }
  
  const dailyFiles = {}; 
  
  //  Split the file by day
  fs.createReadStream(filepath)
    .pipe(Csv())
    .on('data', (row) => {
       // Validation
      if (!validateData(row)) return;

      const timestamp = row.timestamp;
      const value = parseFloat(row.value);

      if (!isNaN(value)) {
        const dayKey = moment(timestamp, 'DD-MM-YYYY HH:mm').format('DD-MM-YYYY');
        //console.log("dddfg",dayKey)

        if (!dailyFiles[dayKey]) {
          dailyFiles[dayKey] = [];
        }

        dailyFiles[dayKey].push({ timestamp, value });

      }
    })
    .on('end', () => {
      // Write each day to a separate file
      Object.keys(dailyFiles).forEach(day => {
        const filePath = `${outputFolder}/${day}.csv`;
        const csvData = 'timestamp,value\n' + dailyFiles[day]
          .map(entry => `${entry.timestamp},${entry.value}`)
          .join('\n');

        fs.writeFileSync(filePath, csvData, 'utf8');
        console.log(` File created: ${filePath}`);
      });
       
      hourlyAverageWithStream();
    });

  
  //-----------Function to Calculate hourly averages for each daily file with stream-----------
  function hourlyAverageWithStream(){
    const allResults = [];

    fs.readdirSync(outputFolder).forEach(file => {
      const filePath = `${outputFolder}/${file}`;
      const hourlyData = {};

      fs.createReadStream(filePath)
        .pipe(Csv())
        .on('data', (row) => {
             // Validation
      if (!validateData(row)) return;
          const timestamp = row.timestamp;
          const value = parseFloat(row.value);

          if (!isNaN(value)) {
            const hourKey = moment(timestamp, 'DD-MM-YYYY HH:mm').startOf('hour').format('DD-MM-YYYY HH:mm');

            if (!hourlyData[hourKey]) {
              hourlyData[hourKey] = { sum: 0, count: 0 };
            }
            hourlyData[hourKey].sum += value;
            hourlyData[hourKey].count += 1;
          }
        })
        .on('end', () => {
          Object.keys(hourlyData).forEach(hour => {
            const avg = (hourlyData[hour].sum / hourlyData[hour].count).toFixed(2);
            allResults.push({ timestamp: hour, average: avg });
          });

          console.log(` Calculation of averages is complete for ${file}`);

          if (allResults.length > 0) { console.log("enter")
            if (file === fs.readdirSync(outputFolder).slice(-1)[0]) {
             
              writeFinalResults(allResults);
            }
          }
        });
    });
  }
}


//------------------------Section B-4-----------------------------

//הפורמט  הוא פורמט אחסון נתונים עמודתי
//הוא מאפשר דחיסה יעילה מאוד של הנתונים, דבר שמפחית את גודל הקובץ בצורה משמעותית 
//כתוצאה מכך, נדרשים פחות משאבים לאחסון.
//משום שהנתונים מאוחסנים בטורים, ניתן לקרוא רק את הטורים הנחוצים לשאילתה ספציפית, ולא את כל השורות
//דבר שמפחית את זמן הקריאה ומשפר את הביצועים, .
//הוא אידיאלי לסביבות עבודה גדולות עם נתונים רבים ומאפשר עבודה על כמויות גדולות של נתונים במהירות.


//--------------Function to process daily chunks wite PARQUET------------------------
async function processDailyChunksWithPARQUET() {
  if (!fs.existsSync(outputFolder)) {
    fs.mkdirSync(outputFolder);
  }

  const reader = await parquet.ParquetReader.openFile(filepathPARQUET);//Opens the file for reading.
  const cursor = reader.getCursor();//Reading line by line
  const dailyFiles = {};

  let row;
  while ((row = await cursor.next())) {//Going over the lines
    if (!validateData(row)) return;
    const timestamp = row.timestamp;
    const value = parseFloat(row.value);

    if (!isNaN(value)) {
      const dayKey = moment(timestamp, 'DD-MM-YYYY HH:mm').format('DD-MM-YYYY');
      if (!dailyFiles[dayKey]) {
        dailyFiles[dayKey] = [];
      }
      dailyFiles[dayKey].push({ timestamp, value });
    }
  }
  await reader.close();

  for (const day in dailyFiles) {
    const filePath = `${outputFolder}/${day}.csv`;
    const csvData = 'timestamp,value\n' + dailyFiles[day]
      .map(entry => `${entry.timestamp},${entry.value}`)
      .join('\n');

    fs.writeFileSync(filePath, csvData, 'utf8');
    console.log(`File created: ${filePath}`);
  }

  await hourlyAverageWithStream();
}



// -----------------Function to Create final CSV file with all averages--------------------------
function writeFinalResults(results) {
  const sortedResults = results.sort((a, b) => moment(a.timestamp) - moment(b.timestamp));

  const outputFilePath = 'hourly_averages.csv';
  const csvData = 'timestamp,average\n' + sortedResults
    .map(entry => `${entry.timestamp},${entry.average}`)
    .join('\n');

  fs.writeFileSync(outputFilePath, csvData, 'utf8');
  console.log(` File created: ${outputFilePath}`);
 }


//------------------Calling functions--------------------
//Section B-1
//calculateHourlyAverages();
//Section B-2
processDailyChunksWithStream();
//Section B-3
//processDailyChunksNoStream();
//Section B-4
//processDailyChunksWithPARQUET


//---------------File Error Update-------------------
setTimeout(() => {
  console.log(' Testing has been completed.');
  if (invalidTimestamps.length > 0) {
    console.log(` Found ${invalidTimestamps.length} invalid timestamps:`, invalidTimestamps);
  }
  if (duplicateRows.length > 0) {
    console.log(` Found ${duplicateRows.length} duplicate rows:`, duplicateRows);
  }
  if (missingValues.length > 0) {
    console.log(` Found ${missingValues.length} rows with missing values.`,missingValues);
  }
  if (invalidValues.length > 0) {
    console.log(` Found ${invalidValues.length} non-numeric values:`, invalidValues);
  }
  if (invalidTimestamps.length === 0 && duplicateRows.length === 0 && missingValues.length === 0 && invalidValues.length === 0) {
    console.log(' All data is valid!');
  }
}, 7000);
