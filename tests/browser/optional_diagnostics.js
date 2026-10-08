async (page) => {
  const fixtures = '/tmp/dbcheck-optional-diagnostics-fixtures';
  const section = page.locator('#new-report');
  const picker = section.getByLabel('选择 ZIP 采集包');
  const row = (name) => section.locator('div.rounded-xl').filter({ has: page.getByText(name, { exact: true }) });
  const check = (condition, message) => { if (!condition) throw new Error(message); };
  const addZips = (names) => picker.setInputFiles(names.map((name) => `${fixtures}/${name}`));
  async function attach(name, kind, names) {
    const button = row(name).getByRole('button', { name: `添加 ${kind}` });
    await button.focus();
    const chooser = page.waitForEvent('filechooser');
    await button.press('Enter');
    await (await chooser).setFiles(names.map((file) => `${fixtures}/${file}`));
    for (const file of names) await row(name).getByText(file, { exact: true }).waitFor();
  }
  async function generate(name) {
    await section.getByRole('button', { name: /生成 \d+ 份报告/ }).click();
    await section.getByRole('heading', { name: '报告好了。' }).waitFor({ timeout: 120000 });
    const pending = page.waitForEvent('download');
    await section.getByRole('button', { name: /下载/ }).click();
    await (await pending).saveAs(`${fixtures}/${name}.zip`);
    await section.getByRole('button', { name: /再来一份/ }).click();
  }
  async function zipAndOracleJourney() {
    check(await section.getByRole('button', { name: /添加 (AWR|WDR)/ }).count() === 0, 'empty screen has diagnostic controls');
    check(await picker.getAttribute('accept') === '.zip', 'main picker accepts non-ZIP files');
    await addZips(['broken.zip']);
    await row('broken.zip').getByText('不是有效的 ZIP 文件').waitFor();
    check(await section.getByRole('button', { name: /添加 (AWR|WDR)/ }).count() === 0, 'invalid ZIP has diagnostic controls');
    await row('broken.zip').getByRole('button', { name: '移除', exact: true }).click();
    await picker.setInputFiles(`${fixtures}/awr.html`);
    await page.getByText(/这里只接受 ZIP 采集包/).waitFor();
    check(await section.getByText('awr.html', { exact: true }).count() === 0, 'HTML created an unpaired item');
    await addZips(['oracle.zip']);
    await row('oracle.zip').getByRole('button', { name: '添加 AWR' }).waitFor();
    await generate('no-attachment');
    await addZips(['oracle.zip']);
    await attach('oracle.zip', 'AWR', ['awr.html']);
    check(await row('oracle.zip').getByRole('button', { name: '添加 AWR' }).isDisabled(), 'AWR replacement silently allowed');
    await row('oracle.zip').getByRole('button', { name: '移除 awr.html' }).click();
    await attach('oracle.zip', 'AWR', ['awr.html']);
    await generate('one-awr');
  }

  async function gaussAndMixedJourney() {
    await addZips(['gaussdb.zip']);
    await attach('gaussdb.zip', 'WDR', ['wdr-one.html', 'wdr-two.htm']);
    await row('gaussdb.zip').getByRole('button', { name: '移除 wdr-one.html' }).click();
    await attach('gaussdb.zip', 'WDR', ['wdr-one.html']);
    await generate('multiple-wdr');
    await addZips(['mysql.zip', 'oracle.zip', 'gaussdb.zip']);
    await row('oracle.zip').getByRole('button', { name: '添加 AWR' }).waitFor();
    check(await row('mysql.zip').getByRole('button', { name: /添加/ }).count() === 0, 'MySQL has diagnostic controls');
    await attach('oracle.zip', 'AWR', ['awr.html']);
    await attach('gaussdb.zip', 'WDR', ['wdr-one.html', 'wdr-two.htm']);
    await row('mysql.zip').getByRole('button', { name: '移除', exact: true }).click();
    check(await row('oracle.zip').getByText('awr.html', { exact: true }).count() === 1, 'Oracle attachment lost after removing another item');
    check(await row('gaussdb.zip').getByText('wdr-two.htm', { exact: true }).count() === 1, 'WDR attachment lost after removing another item');
    await addZips(['mysql.zip']);
    await generate('mixed-batch');
  }

  await zipAndOracleJourney();
  await gaussAndMixedJourney();
  console.log('Optional diagnostic browser selection and real generation checks passed.');
}
