export function buildKey<T, K extends keyof T>(obj: T, ...properties: K[]): Pick<T, K> {
  if (properties.length === 0) {
    return { ...obj };
  }
  const result: Pick<T, K> = {} as Pick<T, K>;
  properties.forEach((property) => {
    result[property] = obj[property];
  });
  return result;
}
