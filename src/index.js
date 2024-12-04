import express, { response } from 'express';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite'

import { applyRateLimiting, applyLooseCORSPolicy, applyBodyParsing, applyLogging, applyErrorCatching } from './api-middleware.js'

const app = express();
const port = 8000;

const INSERT_POINTS_SQL = "INSERT INTO Points(payer, points, timestamp) VALUES (?, ?, ?) RETURNING id;"
const GET_POINTS_SQL = "SELECT * FROM Points ORDER BY timestamp;"
const UPDATE_POINTS_SQL = "UPDATE Points SET points = ? WHERE id = ?;"

const db = await open({
    filename: "./db.db",
    driver: sqlite3.Database
});

await db.exec("CREATE TABLE IF NOT EXISTS Points(id INTEGER PRIMARY KEY UNIQUE, payer TEXT NOT NULL, points INTEGER NOT NULL, timestamp TEXT NOT NULL);")

applyRateLimiting(app);
applyLooseCORSPolicy(app);
applyBodyParsing(app);
applyLogging(app);

app.post('/add', async (req, res) => {
    /*
        When a user has points added, we will use an /add route that accepts a transaction which contains
        how many points will be added, what payer the points will be added through, and the timestamp for when the
        transaction takes place. The request body for this endpoint will look like the following:
        {
            "payer" : "DANNON",
            "points" : 5000,
            "timestamp" : "2020-11-02T14:00:00Z"
        }
        Your service should keep track of each transaction when a new one is added. If the transaction was added
        successfully, then your endpoint should respond with a status code of 200. You do not need to include a
        response body.
    */
    const payer = req.body.payer;
    const points = req.body.points
    const timestamp = req.body.timestamp

    if (!payer) {
        res.status(400).send({
            msg: "You must specify a payer!"
        })
    } else if (!points) {
        res.status(400).send({
            msg: "You must specify points to add!"
        })
    } else if (!timestamp) {
        res.status(400).send({
            msg: "You must specify a timestamp!"
        })
    } else {
        try {
            const ret = await db.get(INSERT_POINTS_SQL, payer, points, timestamp);
            res.status(200).send({
                msg: "Successfully added!",
                id: ret.id
            })
        } catch (e) {
            console.error(e);
            res.status(500).send({
                msg: "Something went wrong!"
            });
        }
    }
})
                 
app.post('/spend', async (req, res) => {
    /*
        When a user goes to spend their points, they are not aware of what payer their points were added
        through. Because of this, your request body should look like
        {"points" : 5000}
        When a spend request comes in, your service should use the following rules to decide which payer to spend points
        through:
        ● We want the oldest points to be spent first (oldest based on transaction timestamp, not the order they’re
        received)
        ● We want no payer's points to go negative
        If a request was made to spend more points than what a user has in total, then we should return a status
        code of 400 and a message saying the user doesn’t have enough points. This can be done through a text
        response rather than a JSON response. If the user does have enough points, then the above rules should be
        applied to decide which payer the above points should be spent through. After your service has successfully
        calculated who to remove points from, the endpoint should respond with a status code of 200 and a list of
        payer names and the number of points that were subtracted. An example of a response body looks like the
        following:
        [
            { "payer": "DANNON", "points": -100 },
            { "payer": "UNILEVER", "points": -200 },
            { "payer": "MILLER COORS", "points": -4,700 }
        ]
    */
    let points = req.body.points

    if (!points) {
        res.status(400).send({
            msg: "You must specify points to spend!"
        })
    } else {
        try {
            const pointsRecord = await db.all(GET_POINTS_SQL);
            const spendSummary = {}

            // Check if the user has enough balance to make the request
            const totalBalance = pointsRecord.reduce((prev, data) => prev + data.points, 0)
            if(totalBalance < points) {
                res.status(400).send("The user doesn't have enough points!")
                return
            }

            // Spend points in order of timestamps
            for (const data of pointsRecord) {
                if(points <= 0) {
                    break
                }
                if(data.points > 0) {
                    let pointsDeduced = Math.min(points, data.points)
                    await db.run(UPDATE_POINTS_SQL, data.points - pointsDeduced, data.id) // Update db record
                    points -= pointsDeduced

                    // Update spending summary for return
                    spendSummary[data.payer]?
                        spendSummary[data.payer] -= pointsDeduced:
                        spendSummary[data.payer] = (-1) * pointsDeduced
                }
            }

            res.status(200).send(Object.entries(spendSummary).map(([key, value]) => ({ payer:key, points:value })))
        } catch (e) {
            console.error(e);
            res.status(500).send({
                msg: "Something went wrong!"
            });
        }
    }
})


app.get('/balance', async (req, res) => {
    /*
        Route: /balance
        Method: GET
        Description: This route should return a map of points the user has in their account based on the payer they were
        added through. This endpoint can be used to see how many points the user has from each payer at any given
        time. Because this is a GET request, there is no need for a request body. This endpoint should always
        return a 200 and give a response body similar to the following:
        {
            "DANNON": 1000,
            ”UNILEVER” : 0,
            "MILLER COORS": 5300
        }
    */
    try {
        const pointsRecord = await db.all(GET_POINTS_SQL);
        const result = {}
        pointsRecord.forEach(data => result[data.payer] === undefined? result[data.payer] = data.points : result[data.payer] += data.points)
        res.status(200).send(result)
    } catch (e) {
        console.error(e);
        res.status(500).send({
            msg: "Something went wrong!"
        });
    }
})

applyErrorCatching(app);

app.listen(port, () => {
    console.log(`My API has been opened on :${port}`)
});
