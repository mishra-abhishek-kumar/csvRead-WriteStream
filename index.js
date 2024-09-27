const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const inputCsvFilePath = 'users.csv';
const outputCsvFilePath = 'users_uppercase.csv';

const batchSize = 10000;

// Define the CSV writer configuration for the output file
const csvWriter = createCsvWriter({
  path: outputCsvFilePath,
  header: [
    { id: 'userId', title: 'User ID' },
    { id: 'firstName', title: 'First Name' },
    { id: 'email', title: 'Email' },
    { id: 'password', title: 'Password' },
    { id: 'birthdate', title: 'Birthdate' },
    { id: 'registeredAt', title: 'Registered At' },
  ],
});

async function processCsv() {
  return new Promise((resolve, reject) => {
    const firstNameCounts = {};
    const usersBatch = [];
    let recordsProcessed = 0;

    const readStream = fs.createReadStream(inputCsvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        // Count occurrences of each firstName
        const firstNameUpper = row.firstName ? row.firstName.toUpperCase() : 'UNKNOWN';
        
        if (firstNameCounts[firstNameUpper]) {
          firstNameCounts[firstNameUpper]++;
        } else {
          firstNameCounts[firstNameUpper] = 1;
        }

        // Update the row with the uppercase firstName
        const updatedRow = {
          ...row,
          firstName: firstNameUpper,
        };

        usersBatch.push(updatedRow);

        // If batch is full, write to the output CSV and clear the batch
        if (usersBatch.length >= batchSize) {
          readStream.pause(); // Pause reading to process the batch

          csvWriter.writeRecords(usersBatch)
            .then(() => {
              recordsProcessed += usersBatch.length;
              console.log(`${recordsProcessed} records processed and written.`);
              usersBatch.length = 0; // Clear the batch
              readStream.resume(); // Resume reading
            })
            .catch((error) => reject(error));
        }
      })
      .on('end', () => {
        if (usersBatch.length > 0) {
          // Write remaining records
          csvWriter.writeRecords(usersBatch)
            .then(() => {
              recordsProcessed += usersBatch.length;
              console.log(`${recordsProcessed} records processed and written.`);
              console.log('CSV file processing complete.');
              console.log('First name counts:', firstNameCounts);
              resolve();
            })
            .catch((error) => reject(error));
        } else {
          console.log('CSV file processing complete.');
          console.log('First name counts:', firstNameCounts);
          resolve();
        }
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

processCsv().catch((err) => {
  console.error('Error processing CSV file:', err);
});
