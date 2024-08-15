package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.cache.savemode;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Optional;

@Slf4j
public class CacheSaveModeHandler extends DefaultSaveModeHandler {

    public boolean createIndex;

    public CacheSaveModeHandler(
            @Nonnull SchemaSaveMode schemaSaveMode,
            @Nonnull DataSaveMode dataSaveMode,
            @Nonnull Catalog catalog,
            @Nonnull TablePath tablePath,
            @Nullable CatalogTable catalogTable,
            @Nullable String customSql,
            boolean createIndex) {
        super(schemaSaveMode, dataSaveMode, catalog, tablePath, catalogTable, customSql);
        this.createIndex = createIndex;
    }

    @Override
    protected void createTable() {
        try {
            log.info(
                    "Creating table {} with action {}",
                    tablePath,
                    catalog.previewAction(
                            Catalog.ActionType.CREATE_TABLE,
                            tablePath,
                            Optional.ofNullable(catalogTable)));
            catalog.createTable(tablePath, catalogTable, true, createIndex);
        } catch (UnsupportedOperationException ignore) {
            log.info("Creating table {}", tablePath);
        }
    }
}
