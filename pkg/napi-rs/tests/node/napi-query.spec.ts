import { expect, it } from 'vitest'
import { Glue } from '../../index'

it('Node: basic query operations', async () => {
  const glue = new Glue()

  // create table
  const create = await glue.query('CREATE TABLE NodeFoo (id INTEGER)')
  expect(create).toBeDefined()
  expect(create[0].type).toBe('CREATE TABLE')

  // insert
  const insert = await glue.query('INSERT INTO NodeFoo VALUES (10), (20)')
  expect(insert[0].type).toBe('INSERT')
  expect(insert[0].affected).toBe(2)

  // select
  const select = await glue.query('SELECT * FROM NodeFoo')
  expect(select[0].type).toBe('SELECT')
  expect(Array.isArray(select[0].rows)).toBe(true)
  expect(select[0].rows.length).toBeGreaterThanOrEqual(2)
})
