import Service from '../index.js';
import section from 'section-tests';
import assert from 'assert';
import log from 'ee-log';
import ServiceManager from '@infect/rda-service-manager';
import { ShardedDataSet } from '@infect/rda-fixtures';
import HTTP2Client from '@distributed-systems/http2-client';


const host = 'http://l.dns.porn';



section('Cluster Controller', (section) => {
    let sm;
    let dataSet;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=*'.split(' '),
        });

        await sm.startServices('@infect/rda-service-registry');
        await sm.startServices('@infect/infect-rda-sample-storage', '@infect/rda-cluster-service', '@infect/rda-lock-service');
        await sm.startServices('@infect/rda-compute-service', '@infect/rda-compute-service', '@infect/rda-compute-service', '@infect/rda-compute-service');


        // add fixtures
        dataSet = new ShardedDataSet();
        await dataSet.create();
    });



    section.test('create cluster', async() => {
        section.setTimeout(5000);

        const service = new Service();
        const client = new HTTP2Client();
        await service.load();

        const clusterResponse = await client.post(`${host}:${service.getPort()}/rda-coordinator.cluster`).expect(201).send({
            dataSource: 'infect-rda-sample-storage',
            dataSet: dataSet.dataSetId,
            modelPrefix: 'Infect',
        });

        const data = await clusterResponse.getData();

        assert(data);
        assert(data.clusterId);

        await section.wait(1000);
        await service.end();
        await client.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});
