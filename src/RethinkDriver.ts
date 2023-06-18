import rethinkdb, { Connection } from "rethinkdb";
import { Entry, StorageDriver } from "@pawel-kuznik/datasack";
import { EventHandler, EmitterLike, Emitter } from "@pawel-kuznik/iventy";
import { EventHandlerUninstaller } from "@pawel-kuznik/iventy/build/lib/Channel";

const rethinkDefaultChangeFeedOptions = {
    includeInitial: false,
    squash: false,
    includeOffsets: false,
    includeTypes: false,
    includeStates: false,
    changefeedQueueSize: 100000
};


export class RethinkDriver<TEntry extends Entry = Entry, TFilter extends object = {}> implements StorageDriver<TEntry, TFilter> {

    private _connection: Connection;
    private _databaseName: string;
    private _tableName: string;

    private _emitter: Emitter = new Emitter();

    constructor(connection: Connection, database: string, table: string) {

        this._connection = connection;
        this._databaseName = database;
        this._tableName = table;


    }

    async fetch(id: string): Promise<TEntry | undefined> {

        const data = await rethinkdb
            .db(this._databaseName)
            .table(this._tableName)
            .get(id)
            .run(this._connection);

        return data ? data as TEntry : undefined;
    }

    async insert(input: TEntry): Promise<void> {
        
        return rethinkdb
            .db(this._databaseName)
            .table(this._tableName)
            .insert(input, { conflict: "replace" })
            .run(this._connection)
            .then(() => { });
    }

    async find(filter?: {} | undefined): Promise<TEntry[]> {
        
        return rethinkdb
            .db(this._databaseName)
            .table(this._tableName)
            .filter(filter as { [key: string]: string })
            .run(this._connection)
            .then(result => result.toArray());
    }

    async update(input: TEntry): Promise<void> {
        
        return rethinkdb
            .db(this._databaseName)
            .table(this._tableName)
            .insert(input, { conflict: "update" })
            .run(this._connection)
            .then(() => { });
    }

    async delete(input: string | TEntry): Promise<void> {

        const id = typeof(input) === "string" ? input : input.id;

        return rethinkdb
            .db(this._databaseName)
            .table(this._tableName)
            .get(id)
            .delete()
            .run(this._connection)
            .then(() => { });
    }

    async insertCollection(input: TEntry[]): Promise<void> {

        return rethinkdb
            .db(this._databaseName)
            .table(this._tableName)
            .insert(input, { conflict: "replace" })
            .run(this._connection)
            .then(() => { });
    }

    async updateCollection(input: TEntry[]): Promise<void> {
        
        return rethinkdb
            .db(this._databaseName)
            .table(this._tableName)
            .insert(input, { conflict: "update" })
            .run(this._connection)
            .then(() => { });
    }

    async deleteCollection(input: TEntry[] | string[]): Promise<void> {

        const ids = input.map(input => typeof(input) === "string" ? input : input.id);

        return rethinkdb
            .db(this._databaseName)
            .table(this._tableName)
            .getAll(...ids)
            .delete()
            .run(this._connection)
            .then(() => { });
    }


    async dispose(): Promise<void> {
        
        return Promise.resolve();
    }

    handle(name: string, callback: EventHandler): EventHandlerUninstaller {
        
        return this._emitter.handle(name, callback);
    }

    on(name: string, callback: EventHandler): EmitterLike {
        
        this._emitter.on(name, callback);
        return this;
    }

    off(name: string, callback: EventHandler | null): EmitterLike {
        
        this._emitter.off(name, callback);
        return this;
    }
};
