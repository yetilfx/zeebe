/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */

import {observer} from 'mobx-react';
import isNil from 'lodash/isNil';
import {CopiableProcessID} from 'App/Processes/CopiableProcessID';
import {ProcessOperations} from '../../ProcessOperations';
import {Restricted} from 'modules/components/Restricted';
import {processesStore} from 'modules/stores/processes/processes.list';
import {PanelHeader, Dd, Dl, Dt} from './styled';
import {IS_VERSION_TAG_ENABLED} from 'modules/feature-flags';

type ProcessDetails = {
  bpmnProcessId?: string;
  processName: string;
  version?: string;
  versionTag?: string | null;
};

type DiagramHeaderProps = {
  processDetails: ProcessDetails;
  processDefinitionId?: string;
  tenant?: string;
  isVersionSelected: boolean;
  panelHeaderRef?: React.RefObject<HTMLDivElement>;
};

const DiagramHeader: React.FC<DiagramHeaderProps> = observer(
  ({
    processDetails,
    processDefinitionId,
    tenant,
    isVersionSelected,
    panelHeaderRef,
  }) => {
    const {processName, bpmnProcessId, version, versionTag} = processDetails;
    const hasVersionTag = !isNil(versionTag);
    const hasSelectedProcess = bpmnProcessId !== undefined;

    return (
      <PanelHeader
        title={!hasSelectedProcess ? 'Process' : undefined}
        ref={panelHeaderRef}
      >
        {hasSelectedProcess && (
          <>
            <Dl>
              <Dt>Process name</Dt>
              <Dd title={processName} role="heading">
                {processName}
              </Dd>
            </Dl>

            <Dl>
              <Dt>Process ID</Dt>
              <Dd>
                <CopiableProcessID bpmnProcessId={bpmnProcessId} />
              </Dd>
            </Dl>

            {hasVersionTag && IS_VERSION_TAG_ENABLED && (
              <Dl>
                <Dt>Version Tag</Dt>
                <Dd title={versionTag}>{versionTag}</Dd>
              </Dl>
            )}
          </>
        )}

        {isVersionSelected && processDefinitionId !== undefined && (
          <Restricted
            scopes={['write']}
            resourceBasedRestrictions={{
              scopes: ['DELETE'],
              permissions: processesStore.getPermissions(bpmnProcessId, tenant),
            }}
          >
            {version !== undefined && (
              <ProcessOperations
                processDefinitionId={processDefinitionId}
                processName={processName}
                processVersion={version}
              />
            )}
          </Restricted>
        )}
      </PanelHeader>
    );
  },
);

export {DiagramHeader};
