// ------------------- Import libraries -------------------------------
const fs = require('fs');
const XLSX = require('xlsx');
const readline = require('readline');
const path = require('path');
const prompt = require('prompt-sync')();

// ------------------ Convert Excel file to txt -----------------------

// const excelFilePath = 'logs.txt.xlsx';  // Path to the Excel file
// const outputFilePath = 'File.txt';    // Path to the output text file

// function convertExcelToText() {
//     const workbook = XLSX.readFile(excelFilePath);  // Read the entire Excel file
//     const sheetName = workbook.SheetNames[0];  // Select the first sheet
//     const sheet = workbook.Sheets[sheetName]; // Retrieve data from the first sheet
    
//     // Convert the sheet to CSV format
//     const csv = XLSX.utils.sheet_to_csv(sheet);

//     // Write the data to a text file (CSV format)
//     fs.writeFileSync(outputFilePath, csv, 'utf8');
// }

// ------------------------ Define variables -----------------------------
const filePath = 'File.txt';
const totalErrorCounts = {};
const directoryPath = 'chunks'; 


// ----------------------- Simple MinHeap implementation --------------------------
class MinHeap {
    constructor(size) {
        this.size = size;
        this.data = [];
    }

    push(item) {
        this.data.push(item);
        this.data.sort((a, b) => a[1] - b[1]); // Sort by error frequency (value)
        if (this.data.length > this.size) {
            this.data.shift(); // Remove the element with the lowest frequency
        }
    }

    getHeap() {
        return this.data;
    }
}


// -----------------Function to create the directory if it does not exist------------------
const createDirectory = () => {
    if (!fs.existsSync(directoryPath)) {
        fs.mkdirSync(directoryPath);
    }
};


// ---------------1: Function to split the file into chunks and save them in the directory-----------------
const divideFile = () => {
    createDirectory(); 

    const rl = readline.createInterface({
        input: fs.createReadStream(filePath),
        output: process.stdout,
        terminal: false
    });

    let chunk = [];
    let chunkCount = 1;

    rl.on('line', (line) => {
        chunk.push(line);

        if (chunk.length >= 100000) {
            const chunkFilePath = path.join(directoryPath, `chunk_${chunkCount}.txt`);
            fs.writeFile(chunkFilePath, chunk.join('\n'), 'utf8', (err) => {
                if (err) console.error('Error writing chunk file:', err);
                else console.log(`Chunk ${chunkCount} saved`);
            });
            chunkCount++;
            chunk = [];
        }
    });

    rl.on('close', () => {
        if (chunk.length > 0) {
            const chunkFilePath = path.join(directoryPath, `chunk_${chunkCount}.txt`);
            fs.writeFile(chunkFilePath, chunk.join('\n'), 'utf8', (err) => {
                if (err) console.error('Error writing final chunk file:', err);
                else console.log(`Final chunk ${chunkCount} saved`);
            });
        }
    });
};


//--------------------- 2,3: Function to count error occurrences in a file----------------------------
const countErrorCodesInFile = (filePath) => {
    const rl = readline.createInterface({
        input: fs.createReadStream(filePath),
        output: process.stdout,
        terminal: false
    });

    rl.on('line', (line) => {
        const match = line.match(/Error: (\S+)/);
        if (match) {
            const errorCode = match[1];
            totalErrorCounts[errorCode] = (totalErrorCounts[errorCode] || 0) + 1;
        }
    });

    rl.on('close', () => {
        console.log(`Finished processing file: ${path.basename(filePath)}`);
    });
};


// ----------------4: Function to find the top N most frequent errors------------------------
const findTopNErrorCodes = (N) => {
    const minHeap = new MinHeap(N);

    Object.entries(totalErrorCounts).forEach(([errorCode, count]) => {
        minHeap.push([errorCode, count]);
    });

    return minHeap.getHeap();
};


// ----------------Function to read and process all files in the directory---------------------
const processAllFiles = () => {
    fs.readdir(directoryPath, (err, files) => {
        if (err) {
            console.error('Error reading directory:', err);
            return;
        }

        files.forEach(file => {
            const filePath = path.join(directoryPath, file);
            countErrorCodesInFile(filePath);
        });

        setTimeout(() => {
            const N = prompt('Enter number of top errors: ');
            const topNErrorCodes = findTopNErrorCodes(N);
            console.log(`Top ${N} Error Codes:`);
            console.log(topNErrorCodes);
        }, 2000);
    });
};


//-------------------------- Run the code--------------------------------
divideFile();
setTimeout(processAllFiles, 3000);



//-----------------5: Runtime complexity-----------------------------
// M-מספר השורות בקובץ 
// E-מספר השגיאות הקימות
// N-מספר השגיאות השכיחות
//מעבר על כל שורות הקובץ כדי לבדוק את שיכחות השגיאות =O(M)
//מציאת N השיכחים ביותר זה E פעמים להכניס לערימה שזה =O(Elog(N))
//בנתיים:= O(M) + O(Elog(N))
// E יכול ליהיות מקסימום M עם כל שורה זה שגיאה שונה ולכן = O(M)+O(Mlog(N))
//ולכן ס"ה"כ : = O(Mlog(N))


//------------------5: Place complexity----------------------------
//יש מבנה ששומר את כל השגיאות ואת השכיחות של כל שגיאה = O(E)
//ויש את הערימה שהיא =O(N)
//סה"כ: = O(E)








