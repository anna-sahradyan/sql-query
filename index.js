const Connection = require("tedious").Connection;
const Request = require("tedious").Request;
require('dotenv').config();
const config = {
    server: process.env.SERVER,
    authentication: {
        type: "default",
        options: {
            userName: "Codding",
            password: process.env.PASSWORD
        }
    },
    options: {
        port: 1433,
        database: "testdb",
        trustServerCertificate: true
    }
};

const connection = new Connection(config);

connection.connect();

connection.on("connect", (err) => {
    if (err) {
        console.log("Error");
        throw err;
    }
    executeStatement();
});

    function executeStatement() {
        const query = `
        WITH RecursiveSubdepartments AS (
            SELECT id, parent_id, 0 AS level
            FROM subdivisions
            WHERE id IN (
                SELECT subdivision_id
                FROM collaborators
                WHERE id = '710253'
            )
            UNION ALL
            SELECT s.id, s.parent_id, r.level + 1
            FROM subdivisions s
            INNER JOIN RecursiveSubdepartments r ON s.parent_id = r.id
        ),
        EmployeeCounts AS (
            SELECT r.id AS subdivision_id, COUNT(c.id) AS employee_count
            FROM RecursiveSubdepartments r
            LEFT JOIN collaborators c ON r.id = c.subdivision_id
            WHERE c.age < 40 AND LEN(c.name) > 11
            AND c.subdivision_id NOT IN ('100055', '100059')
            GROUP BY r.id
        )
        SELECT c.id, c.name, c.subdivision_id, c.age, r.level, ec.employee_count
        FROM collaborators c
        JOIN RecursiveSubdepartments r ON c.subdivision_id = r.id
        LEFT JOIN EmployeeCounts ec ON r.id = ec.subdivision_id
        WHERE c.age < 40 AND LEN(c.name) > 11
        AND c.subdivision_id NOT IN ('100055', '100059')
        ORDER BY r.parent_id, r.id;
    `;

        const startTime = new Date();
        const request = new Request(query, (error, rowCount, rows) => {
            const endTime = new Date();
            const executionTime = endTime - startTime; // Calculate execution time in milliseconds
            if (error) {
                console.error("Error executing query:", error.message);
            } else {
                console.log("Query executed successfully!");

                // Process the retrieved data here
                const processedData = rows.map(row => {
                    return {
                        id: row.id,
                        name: row.name,
                        subdivision_id: row.subdivision_id,
                        age: row.age,
                        nesting_level: row.level,
                        employee_count: row.employee_count
                    };
                });
                console.log("Processed Data:", processedData);
                console.log("Execution Time:", executionTime, "ms");
            }

            connection.close(); // Close the connection when done
        });

        const rows = [];
        request.on('row', columns => {
            const rowData = {};
            columns.forEach(column => {
                rowData[column.metadata.colName] = column.value;
            });
            rows.push(rowData);
        });

        request.on('requestCompleted', () => {
            console.log("All rows retrieved:", rows);
        });

        console.log("Executing query:", query);
        connection.execSql(request);
    }
