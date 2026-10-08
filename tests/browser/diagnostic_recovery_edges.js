async (page) => {
  const fixtures = '/tmp/dbcheck-diagnostic-recovery-fixtures';
  const section = page.locator('#new-report');
  const row = section.locator('div.rounded-xl').filter({ has: page.getByText('oracle.zip', { exact: true }) });
  const generate = section.getByRole('button', { name: /生成 \d+ 份报告/ });
  async function add() {
    await section.getByLabel('选择 ZIP 采集包').setInputFiles(`${fixtures}/oracle.zip`);
    await row.getByRole('button', { name: '添加 AWR' }).waitFor();
    await row.getByLabel('选择 AWR 文件').setInputFiles(`${fixtures}/malformed.html`);
  }
  async function download(name) {
    await section.getByRole('heading', { name: '报告好了。' }).waitFor({ timeout: 120000 });
    const pending = page.waitForEvent('download');
    await section.getByRole('button', { name: /下载/ }).click();
    await (await pending).saveAs(`${fixtures}/${name}.zip`);
    await section.getByRole('button', { name: /再来一份/ }).click();
  }
  await add();
  await generate.click();
  await row.getByRole('alert').waitFor();
  await row.getByRole('button', { name: '移除 malformed.html' }).click();
  await generate.click();
  await download('removed-optional');

  await add();
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
  await generate.click();
  await arrived;
  await row.getByRole('button', { name: '移除 malformed.html' }).click();
  await row.getByLabel('选择 AWR 文件').setInputFiles(`${fixtures}/awr.html`);
  const pendingResponse = page.waitForResponse((response) => response.url().endsWith('/api/reports/validate') && response.request().method() === 'POST');
  release();
  const response = await pendingResponse;
  if (response.status() !== 200 || (await response.json())[0]?.kind !== 'invalid') throw new Error('Delayed validation did not exercise the real parser');
  await generate.waitFor({ state: 'visible' });
  await page.waitForFunction(() => [...document.querySelectorAll('#new-report button')].some((button) => /生成 \d+ 份报告/.test(button.textContent) && !button.disabled));
  await page.unroute('**/api/reports/validate');
  if (await row.getByRole('alert').count()) throw new Error('stale error applied to replacement');
  await generate.click();
  await download('stale-response-corrected');
  console.log('Optional removal and delayed real validation response recovery passed.');
}
