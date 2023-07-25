/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */

import {observer} from 'mobx-react';
import {Stack} from '@carbon/react';
import {flip, offset} from '@floating-ui/react-dom';
import {Add, ArrowRight, Error} from '@carbon/react/icons';
import isNil from 'lodash/isNil';
import {flowNodeSelectionStore} from 'modules/stores/flowNodeSelection';
import {modificationsStore} from 'modules/stores/modifications';
import {processInstanceDetailsDiagramStore} from 'modules/stores/processInstanceDetailsDiagram';
import {flowNodeMetaDataStore} from 'modules/stores/flowNodeMetaData';
import {modificationRulesStore} from 'modules/stores/modificationRules';
import {tracking} from 'modules/tracking';
import {generateUniqueID} from 'modules/utils/generateUniqueID';
import {Popover} from 'modules/components/Carbon/Popover';
import {
  Title,
  Unsupported,
  SelectedInstanceCount,
  Button,
  InlineLoading,
} from './styled';

type Props = {
  selectedFlowNodeRef?: SVGSVGElement;
  diagramCanvasRef?: React.RefObject<HTMLDivElement>;
};

const ModificationDropdown: React.FC<Props> = observer(
  ({selectedFlowNodeRef, diagramCanvasRef}) => {
    const flowNodeId = flowNodeSelectionStore.state.selection?.flowNodeId;
    const flowNodeInstanceId =
      flowNodeSelectionStore.state.selection?.flowNodeInstanceId ??
      flowNodeMetaDataStore.state.metaData?.flowNodeInstanceId;

    if (
      flowNodeId === undefined ||
      modificationsStore.state.status === 'moving-token'
    ) {
      return null;
    }

    const {selectedRunningInstanceCount} = flowNodeSelectionStore;
    const {canBeModified, availableModifications} = modificationRulesStore;

    return (
      <Popover
        referenceElement={selectedFlowNodeRef}
        middlewareOptions={[
          offset(10),
          flip({
            fallbackPlacements: ['top', 'left', 'right'],
            boundary: diagramCanvasRef?.current ?? undefined,
          }),
        ]}
        variant="arrow"
        autoUpdatePosition
      >
        <Stack gap={3}>
          <Title>Flow Node Modifications</Title>
          <Stack gap={4}>
            {(() => {
              if (flowNodeMetaDataStore.state.status === 'fetching') {
                return <InlineLoading data-testid="dropdown-spinner" />;
              }
              if (!canBeModified) {
                return <Unsupported>Unsupported flow node type</Unsupported>;
              }

              if (availableModifications.length === 0) {
                return <Unsupported>No modifications available</Unsupported>;
              }

              return (
                <>
                  {selectedRunningInstanceCount > 0 && (
                    <SelectedInstanceCount>
                      Selected running instances: {selectedRunningInstanceCount}
                    </SelectedInstanceCount>
                  )}
                  <Stack gap={2}>
                    {availableModifications.includes('add') && (
                      <Button
                        kind="ghost"
                        title="Add single flow node instance"
                        size="sm"
                        renderIcon={Add}
                        onClick={() => {
                          if (
                            processInstanceDetailsDiagramStore.hasMultipleScopes(
                              processInstanceDetailsDiagramStore.getParentFlowNode(
                                flowNodeId,
                              ),
                            )
                          ) {
                            modificationsStore.startAddingToken(flowNodeId);
                          } else {
                            tracking.track({
                              eventName: 'add-token',
                            });

                            modificationsStore.addModification({
                              type: 'token',
                              payload: {
                                operation: 'ADD_TOKEN',
                                scopeId: generateUniqueID(),
                                flowNode: {
                                  id: flowNodeId,
                                  name: processInstanceDetailsDiagramStore.getFlowNodeName(
                                    flowNodeId,
                                  ),
                                },
                                affectedTokenCount: 1,
                                visibleAffectedTokenCount: 1,
                                parentScopeIds:
                                  modificationsStore.generateParentScopeIds(
                                    flowNodeId,
                                  ),
                              },
                            });
                          }

                          flowNodeSelectionStore.clearSelection();
                        }}
                      >
                        Add
                      </Button>
                    )}

                    {availableModifications.includes('cancel-instance') &&
                      !isNil(flowNodeInstanceId) && (
                        <Button
                          kind="ghost"
                          title="Cancel selected instance in this flow node"
                          size="sm"
                          renderIcon={Error}
                          onClick={() => {
                            tracking.track({
                              eventName: 'cancel-token',
                            });

                            modificationsStore.cancelToken(
                              flowNodeId,
                              flowNodeInstanceId,
                            );
                            flowNodeSelectionStore.clearSelection();
                          }}
                        >
                          Cancel instance
                        </Button>
                      )}

                    {availableModifications.includes('cancel-all') && (
                      <Button
                        kind="ghost"
                        title="Cancel all running flow node instances in this flow node"
                        size="sm"
                        renderIcon={Error}
                        onClick={() => {
                          tracking.track({
                            eventName: 'cancel-token',
                          });

                          modificationsStore.cancelAllTokens(flowNodeId);
                          flowNodeSelectionStore.clearSelection();
                        }}
                      >
                        Cancel all
                      </Button>
                    )}

                    {availableModifications.includes('move-instance') &&
                      !isNil(flowNodeInstanceId) && (
                        <Button
                          kind="ghost"
                          title="Move selected instance in this flow node to another target"
                          size="sm"
                          renderIcon={ArrowRight}
                          onClick={() => {
                            modificationsStore.startMovingToken(
                              flowNodeId,
                              flowNodeInstanceId,
                            );
                            flowNodeSelectionStore.clearSelection();
                          }}
                        >
                          Move instance
                        </Button>
                      )}
                    {availableModifications.includes('move-all') && (
                      <Button
                        kind="ghost"
                        title="Move all running instances in this flow node to another target"
                        size="sm"
                        renderIcon={ArrowRight}
                        onClick={() => {
                          modificationsStore.startMovingToken(flowNodeId);
                          flowNodeSelectionStore.clearSelection();
                        }}
                      >
                        Move all
                      </Button>
                    )}
                  </Stack>
                </>
              );
            })()}
          </Stack>
        </Stack>
      </Popover>
    );
  },
);

export {ModificationDropdown};
