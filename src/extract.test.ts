import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';
import {
    extractSirRows,
    mergeMeasurements,
    normalizeHydrometricRows,
    normalizePluviometricRows,
    parseSirTimestamp,
    readMeasurementFile,
    renderIndexHtml,
    renderRainHtml,
    writeCatalogs,
    writeMeasurementIndexes
} from './extract';

dayjs.extend(utc);
dayjs.extend(timezone);

test('extracts the rendered obfuscated hydrometric array', () => {
    const html = `
        <script>
        var VALUES = new Array();
        var abc = new Array();
        abc[0] = new Array("TOS01004782","Bisenzio","Prato (RADIO)","PO","Bisenzio","B","1.50","1.00","0.30","42.70","-0.08","-0.23","-0.27","27/02 03.10","","1");
        function show(_values) {}
        show(abc);
        </script>
    `;

    assert.deepEqual(extractSirRows(html), [[
        'TOS01004782',
        'Bisenzio',
        'Prato (RADIO)',
        'PO',
        'Bisenzio',
        'B',
        '1.50',
        '1.00',
        '0.30',
        '42.70',
        '-0.08',
        '-0.23',
        '-0.27',
        '27/02 03.10',
        '',
        '1'
    ]]);
});

test('normalizes hydrometric rows using the level column', () => {
    const now = dayjs.tz('2024-02-27 10:00', 'Europe/Rome');
    const row = [
        'TOS01004782',
        'Bisenzio',
        'Prato (RADIO)',
        'PO',
        'Bisenzio',
        'B',
        '1.50',
        '1.00',
        '0.30',
        '42.70',
        '-0.08',
        '-0.23',
        '-0.27',
        '27/02 03.10'
    ];

    const { measurements, stations } = normalizeHydrometricRows([row], now);

    assert.equal(measurements.length, 1);
    assert.equal(measurements[0].value, 0.30);
    assert.equal(measurements[0].unit, 'm');
    assert.equal(measurements[0].river, 'Bisenzio');
    assert.equal(stations[0].level, 0.30);
    assert.equal(stations[0].flow, 42.70);
});

test('normalizes pluviometric rows and strips html from numbers', () => {
    const now = dayjs.tz('2024-02-27 10:00', 'Europe/Rome');
    const row = [
        'TOS01001205',
        'Prato Università (GPRS)',
        'PO',
        'B',
        '<b>1.2</b>',
        '2.0',
        '-',
        '<b>9.8</b>',
        '11.0',
        '52.4',
        '90.6',
        '27/02 03.10',
        '-',
        '-',
        '-',
        '-',
        '<b>3</b>',
        '65',
        '1'
    ];

    const { measurements, stations } = normalizePluviometricRows([row], now);

    assert.equal(measurements.length, 6);
    assert.equal(measurements[0].value, 1.2);
    assert.equal(measurements[0].window_minutes, 15);
    assert.equal(measurements[1].window_minutes, 60);
    assert.equal(measurements[2].window_minutes, 360);
    assert.equal(stations[0].altitude_m, 65);
    assert.equal(stations[0].accumulations_mm[15], 1.2);
});

test('parses SIR timestamps in Europe/Rome', () => {
    const now = dayjs.tz('2024-02-27 10:00', 'Europe/Rome');
    const timestamp = parseSirTimestamp('27/02 03.10', now);

    assert.equal(timestamp, dayjs.tz('2024-02-27 03:10', 'Europe/Rome').unix());
});

test('reads legacy newline-delimited json measurements', () => {
    const tempDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'h2occhio-'));
    const filePath = path.join(tempDirectory, '27.json');
    fs.writeFileSync(filePath, [
        '{"sensor_id":"A","timestamp":1,"value":1,"unit":"m","type":"hydrometric","province":"PO","station_name":"One"}',
        '{"sensor_id":"B","timestamp":2,"value":2,"unit":"m","type":"hydrometric","province":"PO","station_name":"Two"}'
    ].join('\n'));

    assert.equal(readMeasurementFile(filePath).length, 2);
});

test('deduplicates repeated measurements by sensor timestamp type and window', () => {
    const incoming = {
        sensor_id: 'TOS01001205',
        timestamp: 1709003400,
        value: 1.2,
        unit: 'mm',
        type: 'pluviometric' as const,
        province: 'PO',
        station_name: 'Prato Università',
        window_minutes: 15 as const
    };

    assert.equal(mergeMeasurements([incoming], [incoming]).length, 1);
});

test('writes indexes, catalogs, and generated index html from fixtures', () => {
    const tempDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'h2occhio-'));
    const baseDataPath = path.join(tempDirectory, 'docs', 'data');
    const now = dayjs.tz('2024-02-27 10:00', 'Europe/Rome');
    const hydrometric = normalizeHydrometricRows([[
        'TOS01004782',
        'Bisenzio',
        'Prato (RADIO)',
        'PO',
        'Bisenzio',
        'B',
        '1.50',
        '1.00',
        '0.30',
        '42.70',
        '-0.08',
        '-0.23',
        '-0.27',
        '27/02 03.10'
    ]], now);
    const pluviometric = normalizePluviometricRows([[
        'TOS01001205',
        'Prato Università (GPRS)',
        'PO',
        'B',
        '<b>1.2</b>',
        '2.0',
        '-',
        '<b>9.8</b>',
        '11.0',
        '52.4',
        '90.6',
        '27/02 03.10',
        '-',
        '-',
        '-',
        '-',
        '<b>3</b>',
        '65',
        '1'
    ]], now);

    writeMeasurementIndexes(baseDataPath, hydrometric.measurements.concat(pluviometric.measurements));
    writeCatalogs(baseDataPath, hydrometric.stations, pluviometric.stations);

    const sensorFile = path.join(baseDataPath, 'sensors', 'TOS01004782', '2024', '2', '27.json');
    const riverFile = path.join(baseDataPath, 'rivers', 'Bisenzio', '2024', '2', '27.json');
    const pluvioSensorFile = path.join(baseDataPath, 'sensors', 'TOS01001205', '2024', '2', '27.json');
    const catalogFile = path.join(baseDataPath, 'catalog', 'pluviometric-stations-by-province.json');
    const html = renderIndexHtml(hydrometric.stations);
    const rainHtml = renderRainHtml(pluviometric.stations);

    assert.equal(JSON.parse(fs.readFileSync(sensorFile, 'utf8')).length, 1);
    assert.equal(JSON.parse(fs.readFileSync(riverFile, 'utf8')).length, 1);
    assert.equal(JSON.parse(fs.readFileSync(pluvioSensorFile, 'utf8')).length, 6);
    assert.equal(JSON.parse(fs.readFileSync(catalogFile, 'utf8')).PO.length, 1);
    assert.match(html, /Bisenzio/);
    assert.match(html, /type=idro/);
    assert.match(html, /rain\.html/);
    assert.match(rainHtml, /Prato Universit/);
    assert.match(rainHtml, /type=pluvio/);
});
