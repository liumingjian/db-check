async (page) => {
  const fixtures = '/tmp/dbcheck-diagnostic-recovery-fixtures';
  const section = page.locator('#new-report');
  const row = (name) => section.locator('div.rounded-xl').filter({ has: page.getByText(name, { exact: true }) });
  const scenarios = [
    ['oracle.zip', 'AWR', 'malformed.html', 'awr.html', 'malformed-awr'],
    ['gaussdb.zip', 'WDR', 'malformed.html', 'wdr-one.html', 'malformed-wdr'],
    ['oracle.zip', 'AWR', 'wdr-one.html', 'awr.html', 'wrong-type-awr'],
    ['gaussdb.zip', 'WDR', 'awr.html', 'wdr-one.html', 'wrong-type-wdr'],
    ['oracle.zip', 'AWR', 'awr-wrong-name.html', 'awr.html', 'awr-name-mismatch'],
    ['oracle.zip', 'AWR', 'awr-wrong-dbid.html', 'awr.html', 'awr-dbid-mismatch'],
    ['gaussdb.zip', 'WDR', 'wdr-wrong-name.html', 'wdr-one.html', 'wdr-name-mismatch'],
  ];
  async function attach(zip, kind, name) {
    await row(zip).getByLabel(`选择 ${kind} 文件`).setInputFiles(`${fixtures}/${name}`);
    await row(zip).getByText(name, { exact: true }).waitFor();
  }
  for (const [zip, kind, bad, good, output] of scenarios) {
    await section.getByLabel('选择 ZIP 采集包').setInputFiles(['oracle.zip', 'gaussdb.zip'].map((name) => `${fixtures}/${name}`));
    await row('oracle.zip').getByRole('button', { name: '添加 AWR' }).waitFor();
    await row('gaussdb.zip').getByRole('button', { name: '添加 WDR' }).waitFor();
    const otherZip = zip === 'oracle.zip' ? 'gaussdb.zip' : 'oracle.zip';
    const otherKind = zip === 'oracle.zip' ? 'WDR' : 'AWR';
    const otherFile = zip === 'oracle.zip' ? 'wdr-two.htm' : 'awr.html';
    await attach(otherZip, otherKind, otherFile);
    await attach(zip, kind, bad);
    await section.getByRole('button', { name: /生成 \d+ 份报告/ }).click();
    await row(zip).getByRole('alert').waitFor({ timeout: 30000 });
    if (!(await row(zip).getByRole('alert').textContent()).includes(bad)) throw new Error('error did not name affected attachment');
    if (await row(otherZip).getByRole('alert').count()) throw new Error('another item was mislabeled');
    if (await row(otherZip).getByText(otherFile, { exact: true }).count() !== 1) throw new Error('other selection lost');
    await row(zip).getByRole('button', { name: `移除 ${bad}`, exact: true }).click();
    if (await row(zip).getByRole('alert').count()) throw new Error('removed attachment retained error');
    await attach(zip, kind, good);
    await section.getByRole('button', { name: /生成 \d+ 份报告/ }).click();
    await section.getByRole('heading', { name: '报告好了。' }).waitFor({ timeout: 120000 });
    const pending = page.waitForEvent('download');
    await section.getByRole('button', { name: /下载/ }).click();
    await (await pending).saveAs(`${fixtures}/${output}.zip`);
    await section.getByRole('button', { name: /再来一份/ }).click();
  }
  console.log('All seven diagnostic failures corrected with retained ZIPs and mixed-batch selections.');
}
