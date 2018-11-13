import {
  frequency as frequencyFormatter,
  duration as durationFormatter,
  convertDurationToObject,
  convertDurationToSingleNumber,
  convertCamelToSpaces,
  convertToMilliseconds,
  getHighlightedText,
  camelCaseToLabel
} from './formatters';
const nbsp = '\u00A0';

describe('frequencyFormatter', () => {
  it('should do nothing for numbers < 1000', () => {
    expect(frequencyFormatter(4)).toBe('4');
    expect(frequencyFormatter(194)).toBe('194');
  });

  it('should handle zero well', () => {
    expect(frequencyFormatter(0)).toBe('0');
  });

  it('should format numbers', () => {
    expect(frequencyFormatter(6934)).toBe(new Intl.NumberFormat().format(6934));
    expect(frequencyFormatter(61934)).toBe(new Intl.NumberFormat().format(61934));
    expect(frequencyFormatter(761934)).toBe(new Intl.NumberFormat().format(761934));
    expect(frequencyFormatter(2349875982)).toBe(new Intl.NumberFormat().format(2349875982));
  });

  it('should use a precision', () => {
    expect(frequencyFormatter(123, 1)).toBe(new Intl.NumberFormat().format(100));
    expect(frequencyFormatter(12345, 2)).toBe(new Intl.NumberFormat().format(12) + ' Thousand');
    expect(frequencyFormatter(12345, 9)).toBe(new Intl.NumberFormat().format(12.345) + ' Thousand');
    expect(frequencyFormatter(12345678, 2)).toBe(new Intl.NumberFormat().format(12) + ' Million');
    expect(frequencyFormatter(12345678, 4)).toBe(
      new Intl.NumberFormat().format(12.35) + ' Million'
    );
  });
});

describe('durationFormatter', () => {
  it('should format ms input into human readable string', () => {
    const time = 27128;

    expect(durationFormatter(time)).toBe(`27s${nbsp}128ms`);
  });

  it('should handle zero well', () => {
    expect(durationFormatter(0)).toBe('0ms');
  });

  it('should single unit well', () => {
    expect(durationFormatter(5 * 60 * 60 * 1000)).toBe('5h');
  });

  it('should handle single millisecond durations', () => {
    expect(durationFormatter(1)).toBe('1ms');
  });

  it('should handle millisecond durations that are below 1', () => {
    expect(durationFormatter({value: 0.2, unit: 'millis'})).toBe('0.2ms');
  });

  it('should not floor millisecond durations only', () => {
    expect(durationFormatter({value: 1.3, unit: 'millis'})).toBe('1.3ms');
    expect(durationFormatter({value: 1.2, unit: 'seconds'})).toBe(`1s${nbsp}200ms`);
  });

  it('should handle a time object', () => {
    expect(durationFormatter({value: 14, unit: 'seconds'})).toBe('14s');
  });

  it('should normalize a time object', () => {
    expect(durationFormatter({value: 15, unit: 'days'})).toBe(`2wk${nbsp}1d`);
  });

  it('should use a precision', () => {
    expect(durationFormatter(123456789, 2)).toBe(`1 day${nbsp}10 hours`);
    expect(durationFormatter(123456789, 4)).toBe(
      `1 day${nbsp}10 hours${nbsp}17 minutes${nbsp}37 seconds`
    );
  });
});

describe('convertDurationToObject', () => {
  it('should return an object with value and unit', () => {
    const result = convertDurationToObject(123);

    expect(result.unit).toBeDefined();
    expect(result.value).toBeDefined();
  });

  it('should convert a millisecond value', () => {
    expect(convertDurationToObject(123)).toEqual({value: '123', unit: 'millis'});
    expect(convertDurationToObject(4 * 60 * 1000)).toEqual({value: '4', unit: 'minutes'});
    expect(convertDurationToObject(1000)).toEqual({value: '1', unit: 'seconds'});
    expect(convertDurationToObject(1001)).toEqual({value: '1001', unit: 'millis'});
  });
});

describe('convertDurationToSingleNumber', () => {
  it('should return simple numbers unprocessed', () => {
    expect(convertDurationToSingleNumber(123)).toBe(123);
  });

  it('should convert duration objects to millis', () => {
    expect(convertDurationToSingleNumber({value: '123', unit: 'millis'})).toBe(123);
    expect(convertDurationToSingleNumber({value: '2', unit: 'minutes'})).toBe(2 * 60 * 1000);
    expect(convertDurationToSingleNumber({value: '1.5', unit: 'seconds'})).toBe(1500);
  });
});

describe('convertCamelToSpaces', () => {
  expect(convertCamelToSpaces('processDefinitionKey')).toBe('Process Definition Key');
  expect(convertCamelToSpaces('engineName')).toBe('Engine Name');
});

describe('convertToMilliseconds', () => {
  expect(convertToMilliseconds(5, 'seconds')).toBe(5000);
  expect(convertToMilliseconds(2, 'months')).toBe(5184000000);
  expect(convertToMilliseconds(3, 'hours')).toBe(10800000);
  expect(convertToMilliseconds(100, 'millis')).toBe(100);
});

describe('camelCaseToLabel', () => {
  expect(camelCaseToLabel('fooBar')).toBe('Foo Bar');
  expect(camelCaseToLabel('startDate')).toBe('Start Date');
});

describe('getHighlightedText', () => {
  it('Should wrap the highlighted text in a span and give it textBlue class', () => {
    const results = getHighlightedText('test text', 'text');
    expect(results[1].props.children).toBe('text');
    expect(results[1].props.className).toBe('textBlue');
  });

  it('Should return the same text as string if the highlight is empty', () => {
    const results = getHighlightedText('test text', '');
    expect(results).toBe('test text');
  });
});
