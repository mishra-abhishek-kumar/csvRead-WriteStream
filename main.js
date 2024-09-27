const { faker } = require('@faker-js/faker');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// Function to create a single random user
function createRandomUser() {
  return {
    userId: faker.string.uuid(),
    firstName: faker.person.firstName(),
    email: faker.internet.email(),
    password: faker.internet.password(),
    birthdate: faker.date.birthdate(),
    registeredAt: faker.date.past(),
  };
}

// Define the CSV file path
const csvFilePath = 'users.csv';

// Define the CSV writer configuration
const csvWriter = createCsvWriter({
  path: csvFilePath,
  header: [
    { id: 'userId', title: 'User ID' },
    { id: 'firstName', title: 'First Name' },
    { id: 'email', title: 'Email' },
    { id: 'password', title: 'Password' },
    { id: 'birthdate', title: 'Birthdate' },
    { id: 'registeredAt', title: 'Registered At' },
  ],
});

// Start measuring time
console.time('User Generation and CSV Writing');

const batchSize = 10000; // Number of users per batch
const totalUsers = 5000; // Total number of users to generate

function generateUsersBatch(count) {
  const users = Array.from({ length: count }, createRandomUser);
  return users;
}

async function writeBatch(batchSize) {
  const usersBatch = generateUsersBatch(batchSize);
  return csvWriter.writeRecords(usersBatch);
}

async function createLargeCsv() {
  for (let i = 0; i < totalUsers; i += batchSize) {
    await writeBatch(batchSize);
    console.log(`Written ${Math.min(i + batchSize, totalUsers)} users to the CSV file`);
  }
  console.timeEnd('User Generation and CSV Writing');
  console.log(`CSV file was written successfully to ${csvFilePath}`);
}

createLargeCsv().catch(err => {
  console.error('Error writing CSV file:', err);
});
