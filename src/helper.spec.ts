import { buildKey } from './helper';

interface Example {
  prop1: string;
  prop2: number;
  prop3: boolean;
}

describe('buildKey', () => {
  it('should pick specified properties from the given object', () => {
    // Arrange
    const obj: Example = { prop1: 'hello', prop2: 42, prop3: true };

    // Act
    const result = buildKey(obj, 'prop1', 'prop3');

    // Assert
    expect(result).toEqual({ prop1: 'hello', prop3: true });
  });

  it('should return a copy of the original object if no properties are specified', () => {
    // Arrange
    const obj: Example = { prop1: 'hello', prop2: 42, prop3: true };

    // Act
    const result = buildKey(obj);

    // Assert
    expect(result).not.toBe(obj);
    expect(result).toMatchObject(obj);
  });

  it('should return an object with only specified properties, ignoring non-existent properties', () => {
    // Arrange
    const obj: Example = { prop1: 'hello', prop2: 42, prop3: true };

    // Act
    const result = buildKey(obj, 'prop1', 'nonExistent' as keyof Example);

    // Assert
    expect(result).toEqual({ prop1: 'hello' });
  });
});
