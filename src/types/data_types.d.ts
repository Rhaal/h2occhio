type River = {
    id: string,
    locationName_1: string,
    locationName_2: string,
    locationName_3: string,
    locationName_4?: string,
    locationName_5?: string,
}

type Sensor = {
    river_id: string,
    available_measurement_types: MeasurementType[],
    location_1: string,
}

type MeasurementType = "hydrometric" | "pluviometric" | "thermic" | "anemometric";

type Measurement = {
    /** Seconds elapsed since Unix epoch at the measurement */
    timestamp: number,
    /** As a float */
    value: number,
    unit: string,
    type: MeasurementType
}

type RawSIRMeasurement = Array<string>;
type RawSIRMeasurements = Array<Array<string>>;
