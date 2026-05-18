package com.dataplatform.processor.services;

import org.apache.iceberg.Table;
import java.util.Map;

public interface CatalogDataService {

    Map<String, Table> getTables();
}