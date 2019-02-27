import styled from 'styled-components';
import SplitPane from 'modules/components/SplitPane';

export const SplitPaneHeader = styled(SplitPane.Pane.Header)`
  display: flex;
  align-items: center;
`;

export const Table = styled.table`
  width: 100%;
`;

export const Tr = styled.tr`
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

export const Td = styled.td`
  display: inline-block;
`;

export const ActionsWrapper = styled.div`
  width: 250px;
`;

export const SplitPaneBody = styled(SplitPane.Pane.Body)`
  position: relative;
`;
