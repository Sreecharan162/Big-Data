import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
public class Bigtable {
  public final String projectId = "g24ai1114-assignment4";
  public final String instanceId = "ail7560";
  public final String tableId = "weather";
  public final String COLUMN_FAMILY = "sensor";
  public BigtableDataClient dataClient;
  public BigtableTableAdminClient adminClient;
  public static void main(String[] args) throws Exception {
    Bigtable bt = new Bigtable();
    bt.connect();
    bt.createTable();
    bt.loadData();
    int temp = bt.query1();
    System.out.println("🌡️Temperature at YVR on 2022-10-01 10AM: " + temp);
    int wind = bt.query2();
    System.out.println("💨Max Windspeed in Portland Sept 2022: " + wind);
    ArrayList < Object[] > readings = bt.query3();
    System.out.println("📋SeaTac Readings for 2022-10-02:");
    for (Object[] row: readings) {
      for (Object val: row) System.out.print(val + " ");
      System.out.println()
    }
    Object[] tempa = bt.query4();
    System.out.println("🔥Hottest reading: " + Arrays.toString(tempa));
    bt.close();
  }
  public void connect() throws Exception {
    BigtableDataSettings dataSettings = BigtableDataSettings.newBuilder()
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .build();
    dataClient = BigtableDataClient.create(dataSettings);
    System.out.println("✅Data client connected.");
    BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .build();
    adminClient = BigtableTableAdminClient.create(adminSettings);
    System.out.println("✅Admin client connected.");
  }
  public void createTable() {
    System.out.println("🔧Creating table: " + tableId);
    try {
      if (!adminClient.exists(tableId)) {
        CreateTableRequest request = CreateTableRequest.of(tableId)
          .addFamily(COLUMN_FAMILY); // 'sensor'
        adminClient.createTable(request);
        System.out.println("✅Table '" + tableId + "' created successfully.");
      } else {
        System.out.println("ℹ️ Table '" + tableId + "' already exists.");
      }
    } catch (Exception e) {
      System.err.println("❌Failed to create table: " + e.getMessage());
    }
  }
  public void loadData() throws Exception {
    loadCSV("seatac.csv", "SEA");
    loadCSV("vancouver.csv", "YVR");
    loadCSV("portland.csv", "PDX");
  }
  private void loadCSV(String fileName, String stationCode) throws IOException {
    System.out.println("📥Loading data from: " + fileName);
    BufferedReader reader = new BufferedReader(new FileReader("/bin/data/" + fileName));
    String line = reader.readLine(); // Skip header
    HashSet < String > seen = new HashSet < > ();
    int count = 0;
    while ((line = reader.readLine()) != null) {
      String[] parts = line.split(",");
      if (parts.length < 8) continue;
      String date = parts[0].trim();
      String time = parts[1].trim();
      String hour = time.split(":")[0];
      String rowKey = stationCode + "#" + date + "#" + hour;
      if (seen.contains(rowKey)) continue; // First row per hour only
      seen.add(rowKey);
      try {
        RowMutation mutation = RowMutation.create(tableId, rowKey)
          .setCell(COLUMN_FAMILY, "temperature", parts[2].trim())
          .setCell(COLUMN_FAMILY, "dewpoint", parts[3].trim())
          .setCell(COLUMN_FAMILY, "humidity", parts[4].trim())
          .setCell(COLUMN_FAMILY, "windspeed", parts[5].trim())
          .setCell(COLUMN_FAMILY, "pressure", parts[6].trim());
        dataClient.mutateRow(mutation);
        count++;
        if (count % 100 == 0) {
          System.out.println("Inserted " + count + " rows so far...");
        }
      } catch (Exception e) {
        System.err.println("⚠️Failed to insert row: " + rowKey + " → " + e.getMessage());
      }
    }
    reader.close();
    System.out.println("✅Finished loading: " + fileName + " | Total rows: " + count);
  }
  public int query1() throws Exception {
    String rowKey = "YVR#2022-10-01#10";
    Row row = dataClient.readRow(tableId, rowKey);
    if (row != null) {
      List < RowCell > cells = row.getCells(COLUMN_FAMILY, "temperature");
      if (!cells.isEmpty()) {
        String tempStr = cells.get(0).getValue().toStringUtf8();
        return Integer.parseInt(tempStr);
      }
    }
    return -999; // Not found or error
  }
  public int query2() throws Exception {
    int maxWind = -1;
    String start = "PDX#2022-09-01";
    String end = "PDX#2022-09-30z"; // 'z' ensures inclusive end of month
    Query query = Query.create(tableId).range(start, end);
    for (Row row: dataClient.readRows(query)) {
      List < RowCell > windCells = row.getCells(COLUMN_FAMILY, "windspeed");
      if (!windCells.isEmpty()) {
        try {
          int val = Integer.parseInt(windCells.get(0).getValue().toStringUtf8());
          if (val > maxWind) maxWind = val;
        } catch (NumberFormatException ignored) {}
      }
    }
    return maxWind;
  }
  public ArrayList < Object[] > query3() throws Exception {
    System.out.println("Executing query #3.");
    ArrayList < Object[] > data = new ArrayList < > ();
    String prefix = "SEA#2022-10-02";
    Query query = Query.create(tableId).prefix(prefix);
    for (Row row: dataClient.readRows(query)) {
      String[] parts = row.getKey().toStringUtf8().split("#");
      String date = parts[1];
      String hour = parts[2];
      String temp = "", dew = "", hum = "", wind = "", press = "";
      for (RowCell cell: row.getCells()) {
        String col = cell.getQualifier().toStringUtf8();
        String val = cell.getValue().toStringUtf8();
        switch (col) {
        case "temperature":
          temp = val;
          break;
        case "dewpoint":
          dew = val;
          break;
        case "humidity":
          hum = val;
          break;
        case "windspeed":
          wind = val;
          break;
        case "pressure":
          press = val;
          break;
        }
      }
      Object[] record = new Object[] {
        date,
        hour,
        Integer.parseInt(temp),
        Integer.parseInt(dew),
        hum,
        wind,
        press
      };
      data.add(record);
    }
    return data;
  }
  public Object[] query4() throws Exception {
    System.out.println("Executing query #4: Highest temperature in July or August 2022.");
    Query query = Query.create(tableId); // full scan
    int maxTemp = Integer.MIN_VALUE;
    String bestStation = "", bestDate = "", bestHour = "";
    for (Row row: dataClient.readRows(query)) {
      String rowKey = row.getKey().toStringUtf8(); // e.g., SEA#2022-07-12#14
      String[] parts = rowKey.split("#");
      if (parts.length < 3) continue;
      String station = parts[0];
      String date = parts[1]; // format: yyyy-MM-dd
      String hour = parts[2];
      // Check if month is July or August
      String[] dateParts = date.split("-");
      if (dateParts.length != 3) continue;
      int year = Integer.parseInt(dateParts[0]);
      int month = Integer.parseInt(dateParts[1]);
      if (year != 2022 || (month != 7 && month != 8)) continue;
      List < RowCell > tempCells = row.getCells(COLUMN_FAMILY, "temperature");
      if (!tempCells.isEmpty()) {
        String tempStr = tempCells.get(0).getValue().toStringUtf8();
        try {
          int temp = Integer.parseInt(tempStr);
          if (temp > maxTemp) {
            maxTemp = temp;
            bestStation = station;
            bestDate = date;
            bestHour = hour;
          }
        } catch (NumberFormatException e) {
          System.err.printf("⚠️Skipping invalid temperature '%s' at row %s\n", tempStr, rowKey);
        }
      }
    }
    Object[] result = new Object[] {
      bestStation,
      bestDate,
      bestHour,
      maxTemp
    };
    return result;
  }
  public void close() {
    if (dataClient != null) dataClient.close();
    if (adminClient != null) adminClient.close();
    System.out.println("Connections closed.");
  }
}