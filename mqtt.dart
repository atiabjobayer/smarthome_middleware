import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:intl/intl.dart';
import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';
import 'package:mysql_client/mysql_client.dart';
import 'package:timezone/standalone.dart' as tz;
import 'package:dotenv/dotenv.dart';

// final client1 = MqttServerClient('10.0.0.88', '');
// final client2 = MqttServerClient('10.0.0.88', '');

// var pongCount = 0; // Pong counter

var env = DotEnv(includePlatformEnvironment: true)..load();

final DB_HOST = env['DB_HOST'].toString();
final DB_PORT = int.parse(env['DB_PORT']!);
final DB_USERNAME = env['DB_USERNAME'].toString();
final DB_PASSWORD = env['DB_PASSWORD'].toString();
final DB_NAME = env['DB_NAME'].toString();

final MQTT_HOST = env['MQTT_HOST'].toString();
final MQTT_USERNAME = env['MQTT_USERNAME'].toString();
final MQTT_PASSWORD = env['MQTT_PASSWORD'].toString();

void main() async {
  // subscribeToMQTT(client1, "Home1_RedLight/STATE");
  // subscribeToMQTT(client2, "Home1_GreenLight/STATE");

  final conn = await MySQLConnection.createConnection(
    host: DB_HOST,
    port: DB_PORT,
    userName: DB_USERNAME,
    password: DB_PASSWORD,
    databaseName: DB_NAME, // optional
  );
  await conn.connect();

  // exit(0);

  var home_query = await conn.execute('SELECT * FROM api_home');
  // await conn.close();

  for (var home_row in home_query.rows) {
    // print(row.assoc());
    var home = home_row.assoc();

    var home_id = home['home_id'];
    var home_name = home['home_name'];

    // print(home_id);

    var switch_query = await conn.execute(
      "SELECT * FROM api_switch WHERE home_id = :id",
      {
        "id": home_id,
      },
    );

    var meter_query = await conn.execute(
      "SELECT * FROM api_meter WHERE home_id = :id",
      {
        "id": home_id,
      },
    );

    // await conn.close();

    for (var meter_row in meter_query.rows) {
      var meter = meter_row.assoc();

      var mqtt_id = meter['mqtt_id'];

      var client = MqttServerClient(MQTT_HOST, '');

      subscribeToMQTTMeter(client, meter['meter_id'] as String,
          "tele/" + mqtt_id! + '/SENSOR', conn);
    }

    for (var switch_row in switch_query.rows) {
      var switchh = switch_row.assoc();
      var mqtt_id = switchh['mqtt_id'];

      var clientState = MqttServerClient(MQTT_HOST, '');
      var clientPower = MqttServerClient(MQTT_HOST, '');
      subscribeToMQTTSwitch(clientState, switchh['switch_id'] as String,
          "tele/" + mqtt_id! + "/STATE", conn);

      subscribeToMQTTSwitch(clientPower, switchh['switch_id'] as String,
          "stat/" + mqtt_id + "/RESULT", conn);
    }
  }

  // await conn.close();
}

void subscribeToMQTTMeter(
    MqttServerClient client, String meterId, String topic, var conn) async {
  // client.logging(on: true);
  client.setProtocolV311();
  client.keepAlivePeriod = 20;
  client.connectTimeoutPeriod = 2000; // milliseconds

  try {
    await client.connect(MQTT_USERNAME, MQTT_PASSWORD);
  } on NoConnectionException catch (e) {
    // print('EXAMPLE::client exception - $e');
    client.disconnect();
  } on SocketException catch (e) {
    // print('EXAMPLE::socket exception - $e');
    client.disconnect();
  }

  if (client.connectionStatus!.state == MqttConnectionState.connected) {
    // print('EXAMPLE::Mosquitto client connected');
  } else {
    // print(
    // 'EXAMPLE::ERROR Mosquitto client connection failed - disconnecting, status is ${client.connectionStatus}');
    client.disconnect();
    exit(-1);
  }

  client.subscribe(topic, MqttQos.atMostOnce);

  client.updates!.listen((List<MqttReceivedMessage<MqttMessage?>>? c) async {
    final recMess = c![0].payload as MqttPublishMessage;
    final pt =
        MqttPublishPayload.bytesToStringAsString(recMess.payload.message);

    // print(
    //     'EXAMPLE::Change notification:: topic is <${c[0].topic}>, payload is <-- $pt -->');
    // print('');

    Map<String, dynamic> telemetryData = jsonDecode(pt);

    // print("Energy meter data paisi");
    // print(telemetryData['ENERGY']);

    // final conn = await MySQLConnection.createConnection(
    //   host: DB_HOST,
    //   port: DB_PORT,
    //   userName: DB_USERNAME,
    //   password: DB_PASSWORD,
    //   databaseName: DB_NAME, // optional
    // );
    // await conn.connect();

    // final DateTime now = DateTime.now().toUtc();
    final DateFormat formatter = DateFormat('yyyy-MM-dd HH:mm:ss');
    var india = tz.getLocation('Asia/Kolkata');
    var now = tz.TZDateTime.now(india);

    var res = await conn.execute(
      "INSERT INTO api_meterlogs(created_at, `usage`, meter_id) VALUES (:created_at, :usage, :meter_id_id)",
      {
        "created_at": formatter.format(now),
        "usage": telemetryData['ENERGY']['Total'],
        "meter_id_id": int.parse(meterId),
      },
    );

    // await conn.close();

    // ekhon energy meter data process koro
  });
}

void subscribeToMQTTSwitch(
    MqttServerClient client, String switchId, String topic, var conn) async {
  // client.logging(on: true);
  client.setProtocolV311();
  client.keepAlivePeriod = 20;
  client.connectTimeoutPeriod = 2000; // milliseconds
  // client.onDisconnected = onDisconnected;
  // client.onConnected = onConnected;
  // client.onSubscribed = onSubscribed;
  // client.pongCallback = pong;

  try {
    await client.connect(MQTT_USERNAME, MQTT_PASSWORD);
  } on NoConnectionException catch (e) {
    // print('EXAMPLE::client exception - $e');
    client.disconnect();
  } on SocketException catch (e) {
    // print('EXAMPLE::socket exception - $e');
    client.disconnect();
  }

  if (client.connectionStatus!.state == MqttConnectionState.connected) {
    // print('EXAMPLE::Mosquitto client connected');
  } else {
    // print(
    //     'EXAMPLE::ERROR Mosquitto client connection failed - disconnecting, status is ${client.connectionStatus}');
    client.disconnect();
    exit(-1);
  }

  client.subscribe(topic, MqttQos.atMostOnce);

  client.updates!.listen((List<MqttReceivedMessage<MqttMessage?>>? c) async {
    await tz.initializeTimeZone();

    final recMess = c![0].payload as MqttPublishMessage;
    final pt =
        MqttPublishPayload.bytesToStringAsString(recMess.payload.message);

    // print(
    //     'EXAMPLE::Change notification:: topic is <${c[0].topic}>, payload is <-- $pt -->');
    // print('');

    Map<String, dynamic> telemetryData = jsonDecode(pt);

    // print("$topic -> " + telemetryData['POWER']);

    String? powerStatus = telemetryData['POWER'];

    if (powerStatus == null || powerStatus.length == 0) {
      return;
    }

    int lastSwitchStatus = await getLastSwitchStatus(switchId, conn);

    // final DateTime now = DateTime.now().toLocal();
    final DateFormat formatter = DateFormat('yyyy-MM-dd HH:mm:ss');

    var india = tz.getLocation('Asia/Kolkata');
    var now = tz.TZDateTime.now(india);

    //   await tz.initializeTimeZones();
    //   var detroit = tz.getLocation('America/Detroit');
    // var now = tz.TZDateTime.now(detroit);

    print("Last switch status for " +
        topic +
        " = " +
        lastSwitchStatus.toString());

    // final conn = await MySQLConnection.createConnection(
    //   host: DB_HOST,
    //   port: DB_PORT,
    //   userName: DB_USERNAME,
    //   password: DB_PASSWORD,
    //   databaseName: DB_NAME, // optional
    // );
    // await conn.connect();

    if ((lastSwitchStatus == 0 || lastSwitchStatus == -1) &&
        telemetryData['POWER'] == "ON") {
      print("Age off chilo pore on hoise");

      var res = await conn.execute(
        "INSERT INTO api_usagelog(turned_on_at, toggle_status, switch_id) VALUES (:turned_on_at, :toggle_status, :switch_id)",
        {
          "turned_on_at": formatter.format(now),
          "toggle_status": "ON",
          "switch_id": switchId,
        },
      );

      // await conn.close();
    } else if (lastSwitchStatus == 1 && telemetryData['POWER'] == "OFF") {
      print("Age on chilo pore off hoise");
      // conn.execute(
      //   "INSERT INTO api_usagelog(turned_off_at, toggle_status, switch_id) VALUES (:turned_off_at, :toggle_status, :switch_id)",
      //   {
      //     "turned_off_at": formatter.format(now),
      //     "toggle_status": "OFF",
      //     "switch_id": switchId,
      //   },
      // );

      var logs_query = await conn.execute(
        "SELECT * from api_usagelog WHERE switch_id = :switchId ORDER BY log_id DESC",
        {
          "switchId": switchId,
        },
      );

      String? logId;

      for (var log_row in logs_query.rows) {
        // print(log_row.assoc());
        logId = log_row.assoc()['log_id'];
        break;
      }

      if (logId == null || logId.length == 0) return;

      var update_query = await conn.execute(
        "UPDATE api_usagelog SET turned_off_at = :turned_off_at , toggle_status = :toggle_status WHERE log_id = :log_id",
        {
          "log_id": logId,
          "turned_off_at": formatter.format(now),
          "toggle_status": "OFF",
        },
      );

      // await conn.close();
    }
  });
}

Future<int> getLastSwitchStatus(String switchId, var conn) async {
  // final conn = await MySQLConnection.createConnection(
  //   host: DB_HOST,
  //   port: DB_PORT,
  //   userName: DB_USERNAME,
  //   password: DB_PASSWORD,
  //   databaseName: DB_NAME, // optional
  // );
  // await conn.connect();

  // print("ekbare upore" + switchId);
  var logs_query = await conn.execute(
    "SELECT * FROM api_usagelog WHERE switch_id = :switch_id ORDER BY log_id DESC limit 1",
    {
      "switch_id": switchId,
    },
  );
  // await conn.close();
  // print("query r age " + switchId);
  // print("Log length = " + logs_query.rows.toList().length.toString());
  int count = 0;
  // for (var log_row in logs_query.rows) {
  //   print(log_row.assoc());
  // }
  // print("query r pore");

  if (logs_query.rows.toList().length < 1) {
    return -1;
  }

  for (var row in logs_query.rows) {
    var usg = row.assoc();

    if (usg['toggle_status'] == "ON") {
      return 1;
    }
    break;
  }

  return 0;
}
