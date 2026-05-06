import fs from 'node:fs';
import https from 'node:https';
import path from 'node:path';
import dayjs, { Dayjs } from 'dayjs';
import customParseFormat from 'dayjs/plugin/customParseFormat';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';

dayjs.extend(customParseFormat);
dayjs.extend(utc);
dayjs.extend(timezone);

const SIR_BASE_URL = 'https://www.sir.toscana.it';
const SIR_HYDROMETRIC_STATIONS_URL = `${SIR_BASE_URL}/monitoraggio/stazioni.php?type=idro`;
const SIR_PLUVIOMETRIC_STATIONS_URL = `${SIR_BASE_URL}/monitoraggio/stazioni.php?type=pluvio`;
const ROME_TIMEZONE = 'Europe/Rome';

type MeasurementWindow = 15 | 60 | 180 | 360 | 720 | 1440 | 2160;

type CatalogStation = {
    sensor_id: string,
    station_name: string,
    province: string,
    alert_zone: string,
    latest_timestamp: number,
    detail_url: string,
    altitude_m?: number
};

type HydrometricStation = CatalogStation & {
    river: string,
    basin: string,
    flow?: number,
    level?: number
};

type PluviometricStation = CatalogStation & {
    accumulations_mm: Partial<Record<MeasurementWindow, number>>
};

type IndexedMeasurement = Measurement & {
    river?: string,
    province: string,
    station_name: string,
    window_minutes?: MeasurementWindow
};

type CollectorResult = {
    hydrometric_measurements: IndexedMeasurement[],
    pluviometric_measurements: IndexedMeasurement[],
    hydrometric_stations: HydrometricStation[],
    pluviometric_stations: PluviometricStation[]
};

const PLUVIOMETRIC_WINDOWS: Array<{ index: number, minutes: MeasurementWindow }> = [
    { index: 4, minutes: 15 },
    { index: 5, minutes: 60 },
    { index: 6, minutes: 180 },
    { index: 7, minutes: 360 },
    { index: 8, minutes: 720 },
    { index: 9, minutes: 1440 },
    { index: 10, minutes: 2160 }
];

const PROVINCE_NAMES: Record<string, string> = {
    AR: 'Arezzo',
    FI: 'Firenze',
    GR: 'Grosseto',
    LI: 'Livorno',
    LU: 'Lucca',
    MS: 'Massa-Carrara',
    PI: 'Pisa',
    PO: 'Prato',
    PT: 'Pistoia',
    SI: 'Siena'
};

export function extractSirRows(source: string, preferredArrayName?: string): RawSIRMeasurements {
    const arrayName = preferredArrayName ?? findRenderedArrayName(source) ?? 'VALUES';
    const rowPattern = new RegExp(`\\b${escapeRegExp(arrayName)}\\s*\\[\\d+\\]\\s*=\\s*new Array\\((.*?)\\);`, 'gs');
    const rows: RawSIRMeasurements = [];
    let match: RegExpExecArray | null;

    while ((match = rowPattern.exec(source)) !== null) {
        rows.push(parseStringArrayArguments(match[1]));
    }

    return rows;
}

export function parseSirTimestamp(input: string, now: Dayjs = dayjs()): number {
    const parsed = dayjs.tz(
        `${now.year()}/${input.trim()}`,
        'YYYY/DD/MM HH.mm',
        ROME_TIMEZONE
    );

    if (!parsed.isValid()) {
        throw new Error(`Unable to parse SIR timestamp: ${input}`);
    }

    if (parsed.isAfter(now.add(1, 'day'))) {
        return parsed.subtract(1, 'year').unix();
    }

    return parsed.unix();
}

export function normalizeHydrometricRows(
    rows: RawSIRMeasurements,
    now: Dayjs = dayjs()
): { measurements: IndexedMeasurement[], stations: HydrometricStation[] } {
    const measurements: IndexedMeasurement[] = [];
    const stations: HydrometricStation[] = [];

    rows.forEach((row) => {
        const sensorId = cleanText(row[0]);
        const river = cleanText(row[1]);
        const stationName = cleanText(row[2]);
        const province = cleanText(row[3]);
        const basin = cleanText(row[4]);
        const alertZone = cleanText(row[5]);
        const timestamp = safeParseSirTimestamp(row[13], now);
        const level = parseNumericValue(row[8]);
        const flow = parseNumericValue(row[9]);

        if (!sensorId || !province || level === null || timestamp === null) {
            return;
        }

        measurements.push({
            sensor_id: sensorId,
            timestamp,
            value: level,
            unit: 'm',
            type: 'hydrometric',
            river,
            province,
            station_name: stationName
        });

        stations.push({
            sensor_id: sensorId,
            station_name: stationName,
            province,
            alert_zone: alertZone,
            latest_timestamp: timestamp,
            detail_url: sirDetailUrl(sensorId, 'idro'),
            river,
            basin,
            level,
            ...(flow === null ? {} : { flow })
        });
    });

    return { measurements, stations };
}

export function normalizePluviometricRows(
    rows: RawSIRMeasurements,
    now: Dayjs = dayjs()
): { measurements: IndexedMeasurement[], stations: PluviometricStation[] } {
    const measurements: IndexedMeasurement[] = [];
    const stations: PluviometricStation[] = [];

    rows.forEach((row) => {
        const sensorId = cleanText(row[0]);
        const stationName = cleanText(row[1]);
        const province = cleanText(row[2]);
        const alertZone = cleanText(row[3]);
        const timestamp = safeParseSirTimestamp(row[11], now);
        const altitude = parseNumericValue(row[17]);
        const accumulations: Partial<Record<MeasurementWindow, number>> = {};

        if (!sensorId || !province || timestamp === null) {
            return;
        }

        PLUVIOMETRIC_WINDOWS.forEach(({ index, minutes }) => {
            const value = parseNumericValue(row[index]);
            if (value === null) {
                return;
            }

            accumulations[minutes] = value;
            measurements.push({
                sensor_id: sensorId,
                timestamp,
                value,
                unit: 'mm',
                type: 'pluviometric',
                window_minutes: minutes,
                province,
                station_name: stationName
            });
        });

        stations.push({
            sensor_id: sensorId,
            station_name: stationName,
            province,
            alert_zone: alertZone,
            latest_timestamp: timestamp,
            detail_url: sirDetailUrl(sensorId, 'pluvio'),
            accumulations_mm: accumulations,
            ...(altitude === null ? {} : { altitude_m: altitude })
        });
    });

    return { measurements, stations };
}

export function readMeasurementFile(filePath: string): IndexedMeasurement[] {
    if (!fs.existsSync(filePath)) {
        return [];
    }

    const content = fs.readFileSync(filePath, 'utf8').trim();
    if (!content) {
        return [];
    }

    if (content.startsWith('[')) {
        return JSON.parse(content) as IndexedMeasurement[];
    }

    return content
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => JSON.parse(line) as IndexedMeasurement);
}

export function mergeMeasurements(
    existingMeasurements: IndexedMeasurement[],
    incomingMeasurements: IndexedMeasurement[]
): IndexedMeasurement[] {
    const measurementsByKey = new Map<string, IndexedMeasurement>();

    existingMeasurements.concat(incomingMeasurements).forEach((measurement) => {
        measurementsByKey.set(measurementKey(measurement), measurement);
    });

    return Array.from(measurementsByKey.values()).sort(compareMeasurements);
}

export function writeMeasurementIndexes(baseDataPath: string, measurements: IndexedMeasurement[]): void {
    const groupedMeasurements = new Map<string, IndexedMeasurement[]>();

    measurements.forEach((measurement) => {
        getMeasurementIndexPaths(baseDataPath, measurement).forEach((filePath) => {
            const grouped = groupedMeasurements.get(filePath) ?? [];
            grouped.push(measurement);
            groupedMeasurements.set(filePath, grouped);
        });
    });

    groupedMeasurements.forEach((incomingMeasurements, filePath) => {
        const mergedMeasurements = mergeMeasurements(readMeasurementFile(filePath), incomingMeasurements);
        fs.mkdirSync(path.dirname(filePath), { recursive: true });
        fs.writeFileSync(filePath, `${JSON.stringify(mergedMeasurements, null, 2)}\n`);
    });
}

export function writeCatalogs(
    baseDataPath: string,
    hydrometricStations: HydrometricStation[],
    pluviometricStations: PluviometricStation[]
): void {
    const catalogPath = path.join(baseDataPath, 'catalog');
    fs.mkdirSync(catalogPath, { recursive: true });

    const hydrometricByRiver = groupBy(
        hydrometricStations.filter((station) => station.river),
        (station) => station.river
    );
    const pluviometricByProvince = groupBy(pluviometricStations, (station) => station.province);

    writeJson(path.join(catalogPath, 'hydrometric-stations.json'), sortStations(hydrometricStations));
    writeJson(path.join(catalogPath, 'hydrometric-stations-by-river.json'), sortGroupedStations(hydrometricByRiver));
    writeJson(path.join(catalogPath, 'pluviometric-stations.json'), sortStations(pluviometricStations));
    writeJson(path.join(catalogPath, 'pluviometric-stations-by-province.json'), sortGroupedStations(pluviometricByProvince));
}

export function renderIndexHtml(hydrometricStations: HydrometricStation[]): string {
    const stationsByProvince = sortGroupedHydrometricStations(groupBy(hydrometricStations, (station) => station.province));
    const provinceSections = Object.entries(stationsByProvince)
        .map(([province, stations]) => renderHydrometricProvinceSection(province, stations))
        .join('\n');

    return `<!DOCTYPE html>
<html lang="it">
    <head>
        <title>H2Occhio - Dati e risorse idrometria e pluviometria - Toscana</title>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta http-equiv="cache-control" content="no-cache" />
        <meta http-equiv="expires" content="0" />
        <meta http-equiv="pragma" content="no-cache" />
        <style>
            body { font-family: sans-serif; line-height: 1.45; margin: 2rem; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border-bottom: 1px solid #ddd; padding: 0.4rem; text-align: left; }
            th { background: #f4f4f4; }
            img { max-width: 100%; height: auto; }
        </style>
    </head>
    <body>
        <h1>Dati e risorse idrometria e pluviometria - Toscana</h1>
        <section>
            <h2>Dati e risorse generali</h2>
            <ul>
                <li><a href="https://www.regione.toscana.it/allertameteo">Allerte meteo attuali</a></li>
                <li><a href="https://www.sir.toscana.it/idrometria-pub">Mappa sensori e livelli di allerta idrogeologici</a></li>
                <li><a href="https://www.sir.toscana.it/pluviometria-pub">Mappa sensori pluviometrici</a></li>
                <li><a href="rain.html">Stazioni pluviometriche</a></li>
                <li><a href="https://www.regione.toscana.it/documents/10180/344853/zone+di+allerta+2015.pdf/16cb0480-e91c-4faf-ada4-a30dc6231d9f">Mappa divisione zone rischio idrogeologico</a></li>
            </ul>
        </section>
        <hr />
        <section>
            <h2>Precipitazioni cumulate complessive</h2>
            <img src="https://www.sir.toscana.it/supports/scripts/image.php?Sottotipo=Toscana&amp;SiglaProduttore=SIR&amp;Nome=CUM1.png" alt="Precipitazioni cumulate complessive in Toscana" />
        </section>
        <hr />
        <section>
            <h2>Fiumi - Idrometria</h2>
${provinceSections}
        </section>
    </body>
</html>
`;
}

export function renderRainHtml(pluviometricStations: PluviometricStation[]): string {
    const stationsByProvince = sortGroupedStations(groupBy(pluviometricStations, (station) => station.province));
    const provinceSections = Object.entries(stationsByProvince)
        .map(([province, stations]) => renderPluviometricProvinceSection(province, stations))
        .join('\n');

    return `<!DOCTYPE html>
<html lang="it">
    <head>
        <title>H2Occhio - Stazioni pluviometriche - Toscana</title>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta http-equiv="cache-control" content="no-cache" />
        <meta http-equiv="expires" content="0" />
        <meta http-equiv="pragma" content="no-cache" />
        <style>
            body { font-family: sans-serif; line-height: 1.45; margin: 2rem; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border-bottom: 1px solid #ddd; padding: 0.4rem; text-align: left; }
            th { background: #f4f4f4; }
        </style>
    </head>
    <body>
        <h1>Stazioni pluviometriche - Toscana</h1>
        <nav>
            <a href="index.html">Fiumi e idrometria</a> |
            <a href="https://www.sir.toscana.it/pluviometria-pub">Mappa SIR pluviometria</a>
        </nav>
        <hr />
        <section>
            <h2>Pioggia - Pluviometria</h2>
${provinceSections}
        </section>
    </body>
</html>
`;
}

export async function collectSirData(baseDataPath = path.join('docs', 'data')): Promise<CollectorResult> {
    const [hydrometricHtml, pluviometricHtml] = await Promise.all([
        fetchText(SIR_HYDROMETRIC_STATIONS_URL),
        fetchText(SIR_PLUVIOMETRIC_STATIONS_URL)
    ]);
    const hydrometric = normalizeHydrometricRows(extractSirRows(hydrometricHtml));
    const pluviometric = normalizePluviometricRows(extractSirRows(pluviometricHtml, 'VALUES'));
    const measurements = hydrometric.measurements.concat(pluviometric.measurements);

    writeMeasurementIndexes(baseDataPath, measurements);
    writeCatalogs(baseDataPath, hydrometric.stations, pluviometric.stations);
    fs.writeFileSync(path.join('docs', 'index.html'), renderIndexHtml(hydrometric.stations));
    fs.writeFileSync(path.join('docs', 'rain.html'), renderRainHtml(pluviometric.stations));

    return {
        hydrometric_measurements: hydrometric.measurements,
        pluviometric_measurements: pluviometric.measurements,
        hydrometric_stations: hydrometric.stations,
        pluviometric_stations: pluviometric.stations
    };
}

async function main(): Promise<void> {
    const result = await collectSirData();
    console.log(
        `Stored ${result.hydrometric_measurements.length} hydrometric measurements ` +
        `and ${result.pluviometric_measurements.length} pluviometric measurements.`
    );
}

function fetchText(url: string): Promise<string> {
    return new Promise((resolve, reject) => {
        https.get(url, (response) => {
            if (
                response.statusCode &&
                response.statusCode >= 300 &&
                response.statusCode < 400 &&
                response.headers.location
            ) {
                response.resume();
                resolve(fetchText(new URL(response.headers.location, url).toString()));
                return;
            }

            if (response.statusCode && response.statusCode >= 400) {
                response.resume();
                reject(new Error(`Request failed for ${url}: HTTP ${response.statusCode}`));
                return;
            }

            response.setEncoding('utf8');
            let body = '';
            response.on('data', (chunk) => {
                body += chunk;
            });
            response.on('end', () => resolve(body));
        }).on('error', reject);
    });
}

function findRenderedArrayName(source: string): string | null {
    const showPattern = /\bshow\s*\(\s*([A-Za-z_$][\w$]*)\s*\)\s*;/g;
    const matches = Array.from(source.matchAll(showPattern));
    const lastMatch = matches[matches.length - 1];

    return lastMatch?.[1] ?? null;
}

function parseStringArrayArguments(input: string): string[] {
    const stringPattern = /"((?:\\.|[^"\\])*)"/g;
    const values: string[] = [];
    let match: RegExpExecArray | null;

    while ((match = stringPattern.exec(input)) !== null) {
        values.push(JSON.parse(`"${match[1]}"`) as string);
    }

    return values;
}

function safeParseSirTimestamp(input: string | undefined, now: Dayjs): number | null {
    if (!input?.trim()) {
        return null;
    }

    try {
        return parseSirTimestamp(input, now);
    } catch {
        return null;
    }
}

function parseNumericValue(input: string | undefined): number | null {
    const cleaned = cleanText(input ?? '').replace(',', '.');
    if (!cleaned || cleaned === '-') {
        return null;
    }

    const parsed = Number.parseFloat(cleaned);
    return Number.isNaN(parsed) ? null : parsed;
}

function cleanText(input: string): string {
    return input
        .replace(/<[^>]+>/g, '')
        .replace(/&nbsp;/g, ' ')
        .trim();
}

function sirDetailUrl(sensorId: string, type: 'idro' | 'pluvio'): string {
    return `${SIR_BASE_URL}/monitoraggio/dettaglio.php?id=${encodeURIComponent(sensorId)}&title=&type=${type}`;
}

function getMeasurementIndexPaths(baseDataPath: string, measurement: IndexedMeasurement): string[] {
    const measurementDate = dayjs.unix(measurement.timestamp).tz(ROME_TIMEZONE);
    const datePath = path.join(
        `${measurementDate.year()}`,
        `${measurementDate.month() + 1}`,
        `${measurementDate.date()}.json`
    );
    const indexPaths = [
        path.join(baseDataPath, 'sensors', safePathSegment(measurement.sensor_id), datePath),
        path.join(baseDataPath, 'provinces', safePathSegment(measurement.province), datePath)
    ];

    if (measurement.type === 'hydrometric' && measurement.river) {
        indexPaths.push(path.join(baseDataPath, 'rivers', safePathSegment(measurement.river), datePath));
    }

    return indexPaths;
}

function safePathSegment(input: string): string {
    return input.replace(/[\\/]/g, '-').trim() || 'unknown';
}

function measurementKey(measurement: IndexedMeasurement): string {
    return [
        measurement.sensor_id,
        measurement.timestamp,
        measurement.type,
        measurement.unit,
        measurement.window_minutes ?? ''
    ].join('|');
}

function compareMeasurements(first: IndexedMeasurement, second: IndexedMeasurement): number {
    return (
        first.timestamp - second.timestamp ||
        first.sensor_id.localeCompare(second.sensor_id) ||
        first.type.localeCompare(second.type) ||
        (first.window_minutes ?? 0) - (second.window_minutes ?? 0)
    );
}

function groupBy<T>(values: T[], getKey: (value: T) => string): Record<string, T[]> {
    return values.reduce<Record<string, T[]>>((grouped, value) => {
        const key = getKey(value).trim() || 'unknown';
        grouped[key] = grouped[key] ?? [];
        grouped[key].push(value);
        return grouped;
    }, {});
}

function sortGroupedStations<T extends CatalogStation>(groupedStations: Record<string, T[]>): Record<string, T[]> {
    return Object.fromEntries(
        Object.entries(groupedStations)
            .sort(([firstKey], [secondKey]) => firstKey.localeCompare(secondKey))
            .map(([key, stations]) => [key, sortStations(stations)])
    );
}

function sortGroupedHydrometricStations(groupedStations: Record<string, HydrometricStation[]>): Record<string, HydrometricStation[]> {
    return Object.fromEntries(
        Object.entries(groupedStations)
            .sort(([firstKey], [secondKey]) => firstKey.localeCompare(secondKey))
            .map(([key, stations]) => [key, sortHydrometricStations(stations)])
    );
}

function sortStations<T extends CatalogStation>(stations: T[]): T[] {
    return stations
        .slice()
        .sort((first, second) =>
            first.province.localeCompare(second.province) ||
            first.station_name.localeCompare(second.station_name) ||
            first.sensor_id.localeCompare(second.sensor_id)
        );
}

function sortHydrometricStations(stations: HydrometricStation[]): HydrometricStation[] {
    return stations
        .slice()
        .sort((first, second) =>
            first.province.localeCompare(second.province) ||
            first.river.localeCompare(second.river) ||
            first.station_name.localeCompare(second.station_name) ||
            first.sensor_id.localeCompare(second.sensor_id)
        );
}

function writeJson(filePath: string, value: unknown): void {
    fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function renderHydrometricProvinceSection(province: string, stations: HydrometricStation[]): string {
    const provinceName = PROVINCE_NAMES[province] ?? province;
    const rows = stations
        .map((station) => `                    <tr>
                        <td>${escapeHtml(station.river)}</td>
                        <td>${escapeHtml(station.station_name)}</td>
                        <td><code>${escapeHtml(station.sensor_id)}</code></td>
                        <td>${escapeHtml(station.alert_zone)}</td>
                        <td>${station.level ?? ''}</td>
                        <td>${station.flow ?? ''}</td>
                        <td>${formatTimestamp(station.latest_timestamp)}</td>
                        <td><a href="${escapeHtml(station.detail_url)}" target="_blank" rel="noopener">Dettaglio SIR</a></td>
                    </tr>`)
        .join('\n');

    return `            <section>
                <h3>${escapeHtml(provinceName)} - ${escapeHtml(province)}</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Fiume</th>
                            <th>Stazione</th>
                            <th>Sensore</th>
                            <th>Zona</th>
                            <th>Livello m</th>
                            <th>Portata</th>
                            <th>Ultimo dato</th>
                            <th>Link</th>
                        </tr>
                    </thead>
                    <tbody>
${rows}
                    </tbody>
                </table>
            </section>`;
}

function renderPluviometricProvinceSection(province: string, stations: PluviometricStation[]): string {
    const provinceName = PROVINCE_NAMES[province] ?? province;
    const rows = stations
        .map((station) => `                    <tr>
                        <td>${escapeHtml(station.station_name)}</td>
                        <td><code>${escapeHtml(station.sensor_id)}</code></td>
                        <td>${escapeHtml(station.alert_zone)}</td>
                        <td>${station.altitude_m ?? ''}</td>
                        <td>${formatTimestamp(station.latest_timestamp)}</td>
                        <td><a href="${escapeHtml(station.detail_url)}" target="_blank" rel="noopener">Dettaglio SIR</a></td>
                    </tr>`)
        .join('\n');

    return `            <section>
                <h3>${escapeHtml(provinceName)} - ${escapeHtml(province)}</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Stazione</th>
                            <th>Sensore</th>
                            <th>Zona</th>
                            <th>Quota m</th>
                            <th>Ultimo dato</th>
                            <th>Link</th>
                        </tr>
                    </thead>
                    <tbody>
${rows}
                    </tbody>
                </table>
            </section>`;
}

function formatTimestamp(timestamp: number): string {
    return dayjs.unix(timestamp).tz(ROME_TIMEZONE).format('YYYY-MM-DD HH:mm');
}

function escapeHtml(input: string): string {
    return input
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function escapeRegExp(input: string): string {
    return input.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

if (require.main === module) {
    main().catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
}
