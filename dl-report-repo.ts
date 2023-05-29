
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
        const pipeline = await this.buildPipelineForMultipleCollections(db, collectionNames);

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

     async buildPipelineForMultipleCollections(db: Db,collectionNames: string[]):Promise< any[]> {
        const pipeline = [];

         const commonFields = await this.getCommonFields(db, collectionNames);

         console.log("----------------------", commonFields)

         pipeline.push({
             $group: {
                 _id: null,
                 data: {
                     $push: commonFields
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
                         { $project: commonFields },
                         { $project: { _id: 0 } }
                     ],
                     as: collection
                 }
             };
             pipeline.push(lookup);

             const unwind = {
                 $unwind: "$" + collection
             };
             pipeline.push(unwind);

             const project = {
                 $project: {
                     data: { $concatArrays: ["$data", ["$" + collection]] }
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

    async getCommonFields(db: Db, collectionNames: string[]): Promise<any> {
        const commonFields = {};

        if (collectionNames.length > 0) {
            const firstCollection = collectionNames[0];
            const firstCollectionFields = await this.getCollectionFields(db, firstCollection);

            for (const field of firstCollectionFields) {
                const isCommonField = collectionNames.slice(1).every((collection) => this.isFieldExists(db, collection, field));
                if (isCommonField) {
                    commonFields[field] = "$" +field;
                }
            }
        }

        return commonFields;
    }

    async getCollectionFields(db: Db, collectionName: string): Promise<string[]> {
        const collection = db.collection(collectionName);
        const fields = await collection.findOne({}, { projection: { _id: 0 } });
        return Object.keys(fields);
    }

    async isFieldExists(db: Db, collectionName: string, field: string): Promise<boolean> {
        const collection = db.collection(collectionName);
        const result = await collection.findOne({ [field]: { $exists: true } }, { projection: { _id: 0, [field]: 1 } });
        return result !== null;
    }

}
