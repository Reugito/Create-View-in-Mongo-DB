import {Db, MongoClient} from "mongodb";
import * as url from "url";
import {Logger} from "@nestjs/common";
import {getEnv} from "../core/config/utils";

export class DlReportRepo {

    private readonly logger = new Logger(DlReportRepo.name)

    async  connectToDB(uri: string): Promise<MongoClient> {
        const client = new MongoClient(uri, );
        await client.connect();
        return client;
    }

    async  findDocuments(db: Db, collectionName: string): Promise<any[]> {
        const collection = db.collection(collectionName);
        const result = await collection.find({}).toArray();
        return result;
    }

    async getCollectionNames(db: Db): Promise<string[]> {
        const cols = await db.listCollections().toArray();
        const collectionNames = cols.map((col) => col.name);
        return collectionNames.filter((name) => name !== 'user_info' && name !== 'system.views');

    }

    async createViewCollection(db: Db, collectionNames: string[]): Promise<boolean> {
        const viewCollectionName = getEnv("DL_BLOCK_DETAILS_DB_VIEW_COLLECTION_NAME")
        if (collectionNames.includes(viewCollectionName)) {
            await this.dropViewCollection(db,viewCollectionName);
            collectionNames = collectionNames.filter((name) => name !== viewCollectionName);
        }

        this.logger.log("Building pipeline  ", collectionNames)
        const pipeline = this.buildPipelineForMultipleCollections(collectionNames);

        const result = await db.createCollection(viewCollectionName, { viewOn: collectionNames[0], pipeline });
        return Boolean(result);
    }

    async  dropViewCollection(db, viewCollectionName) {
        try {
            await db.collection(viewCollectionName).drop();
            this.logger.log(`View collection ${viewCollectionName} dropped successfully`);
        } catch (error) {
            this.logger.log(`Error dropping view collection ${viewCollectionName}: ${error}`);
            throw error;
        }
    }

     buildPipelineForMultipleCollections(collectionNames: string[]): any[] {
        const pipeline = [];

        const commonFields = {
            hash: "$hash",
            blockNumber: "$blockNumber",
            status: "$status",
            gas: "$gas",
            from:"$from",
            to:"$to",
            chain_id:"$chain_id"
        }

        pipeline.push({
            $group: {
                _id: null, // or _id: ""
                data: {
                    $push: {
                       ...commonFields
                    }
                }
            }
        });

        for (let i = 1; i < collectionNames.length; i++) {
            const collection = collectionNames[i];
            const lookup = {
                $lookup: {
                    from: collection,
                    let: {},
                    pipeline: [
                        { $project: { ...commonFields, _id: 0 } }
                    ],
                    as: collection
                }
            };
            pipeline.push(lookup);

            const project = {
                $project: {
                    data: { $concatArrays: ["$data", "$" + collection] },
                    _id: 0
                }
            };
            pipeline.push(project);
        }

        return pipeline;
    }



    async parseMongoDBUri(uri: string) {
        const parsedUrl = url.parse(uri);
        const dbName = parsedUrl.pathname?.substring(1);
        delete parsedUrl.pathname;
        const newConnectionString = url.format(parsedUrl);

        return {
            uri: newConnectionString,
            dbName: dbName
        };
    }



}