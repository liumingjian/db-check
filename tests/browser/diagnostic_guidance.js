async (page) => {
  const fixtures = '/tmp/dbcheck-diagnostic-guidance-fixtures';
  const section = page.locator('#new-report');
  const row = (name) => section.locator('div.rounded-xl').filter({ has: page.getByText(name, { exact: true }) });
  const oracle = row('oracle.zip');
  const gauss = row('gaussdb.zip');
  const generate = section.getByRole('button', { name: /生成 \d+ 份报告/ });
  const validate = section.getByRole('button', { name: '校验附件', exact: true });
  const awrChecked = '数据库名和 DBID 一致；尚无法确认时间范围，请核对 AWR 与本次巡检的时间。';
  const wdrChecked = '数据库名一致；尚无法确认完整数据库身份及时间范围，请核对 WDR 对应的数据库和时间。';
  async function attach(target, kind, file) {
    await target.getByLabel(`选择 ${kind} 文件`).setInputFiles(`${fixtures}/${file}`);
    await target.getByText(file, { exact: true }).waitFor();
  }
  await section.getByLabel('选择 ZIP 采集包').setInputFiles(['oracle.zip', 'gaussdb.zip', 'mysql.zip'].map((name) => `${fixtures}/${name}`));
  await oracle.getByRole('button', { name: '添加 AWR' }).waitFor();
  await gauss.getByRole('button', { name: '添加 WDR' }).waitFor();
  await attach(oracle, 'AWR', 'awr.html');
  await oracle.getByText('awr.html：尚未校验，请核对 AWR 对应的数据库及时间范围。', { exact: true }).waitFor({ timeout: 3000 });
  if (!(await generate.isEnabled())) throw new Error('Manual-check guidance blocked generation');
  await attach(gauss, 'WDR', 'wdr-one.html');
  await validate.click();
  await oracle.getByText(`awr.html：${awrChecked}`, { exact: true }).waitFor({ timeout: 30000 });
  await gauss.getByText(`wdr-one.html：${wdrChecked}`, { exact: true }).waitFor();
  if (await row('mysql.zip').getByText(/尚无法确认|尚未校验/).count()) throw new Error('Guidance leaked to MySQL item');
  await oracle.getByRole('button', { name: '移除 awr.html', exact: true }).click();
  if (await oracle.getByText(/数据库名和 DBID 一致/).count()) throw new Error('Removed AWR retained confirmation');
  await attach(oracle, 'AWR', 'awr-wrong-dbid.html');
  await validate.click();
  await oracle.getByRole('alert').waitFor();
  if (await oracle.getByText(/尚无法确认|尚未校验/).count()) throw new Error('Definite mismatch was downgraded to uncertainty');
  const rejected = page.waitForResponse((response) => response.url().endsWith('/api/reports/validate') && response.request().method() === 'POST');
  await generate.click();
  if ((await (await rejected).json())[0]?.kind !== 'invalid') throw new Error('Mismatch was not rejected by the real service');
  await oracle.getByRole('alert').waitFor();
  if (await section.getByRole('heading', { name: '报告好了。' }).count()) throw new Error('Mismatch admitted a report');
  await oracle.getByRole('button', { name: '移除 awr-wrong-dbid.html', exact: true }).click();
  await attach(oracle, 'AWR', 'awr.html');
  await validate.click();
  await oracle.getByText(`awr.html：${awrChecked}`, { exact: true }).waitFor();
  await generate.click();
  await section.getByRole('heading', { name: '报告好了。' }).waitFor({ timeout: 120000 });
  const pending = page.waitForEvent('download');
  await section.getByRole('button', { name: /下载/ }).click();
  await (await pending).saveAs(`${fixtures}/guidance-mixed.zip`);
  await section.getByRole('button', { name: /再来一份/ }).click();

  await section.getByLabel('选择 ZIP 采集包').setInputFiles(`${fixtures}/oracle.zip`);
  await oracle.getByRole('button', { name: '添加 AWR' }).waitFor();
  await attach(oracle, 'AWR', 'awr.html');
  let release;
  let fetched;
  const held = new Promise((resolve) => { release = resolve; });
  const arrived = new Promise((resolve) => { fetched = resolve; });
  await page.route('**/api/reports/validate', async (route) => {
    if (route.request().method() !== 'POST') { await route.continue(); return; }
    fetched();
    await held;
    await route.continue();
  });
  await validate.click();
  await arrived;
  await oracle.getByRole('button', { name: '移除 awr.html', exact: true }).click();
  await attach(oracle, 'AWR', 'malformed.html');
  const delayed = page.waitForResponse((response) => response.url().endsWith('/api/reports/validate') && response.request().method() === 'POST');
  release();
  if ((await (await delayed).json())[0]?.evidence !== 'database_name_dbid') throw new Error('Delayed result did not contain real confirmation');
  await page.waitForFunction(() => [...document.querySelectorAll('#new-report button')].some((button) => button.textContent.trim() === '校验附件' && !button.disabled));
  await page.unroute('**/api/reports/validate');
  if (await oracle.getByText(/数据库名和 DBID 一致/).count()) throw new Error('Stale confirmation applied to replacement');
  await oracle.getByText('malformed.html：尚未校验，请核对 AWR 对应的数据库及时间范围。', { exact: true }).waitFor();
  await validate.click();
  await oracle.getByRole('alert').waitFor();
  if (await oracle.getByText(/尚无法确认|尚未校验/).count()) throw new Error('Parser failure was downgraded to uncertainty');
  await oracle.getByRole('button', { name: '移除', exact: true }).click();
  if (await section.getByText(/尚无法确认|尚未校验|数据库名和 DBID 一致/).count()) throw new Error('Removed item retained guidance');
  console.log('Manual guidance, limited authoritative evidence, mismatch rejection, removal, replacement, and mixed-batch download passed.');
}
