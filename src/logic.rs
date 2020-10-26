use crate::{
    ClientRequest, Peer, ServerResponse, ServerState, WrappedClientRequest, WrappedServerResponse,
};


impl ServerState {

    pub async fn handle_request(
        &mut self,
        request: WrappedClientRequest,
    ) -> Option<WrappedServerResponse> {
        let peer = self
            .peers
            .entry(request.client_id)
            .or_insert(Peer::new(request.client_id));
        if request.sequence_num == peer.next_sequence_num - 1{
            let prev_response = peer.last_response.as_ref().unwrap().clone();
            warn!(
                "request {:?} is duplicate, older by 1 seq num, re-transmitting response",
                request
            );
            return Some(prev_response);
        } else if request.sequence_num < peer.next_sequence_num {
            warn!(
                "request {:?} is duplicate, older by more than 1 seq num, ignoring",
                request
            );
            return None;
        }
        info!("handling request {:?}", request);
        let response = match request.request {
            ClientRequest::Read => peer.wrap_response(ServerResponse::ReadOk {
                contents: self.log.clone(),
            }),
            ClientRequest::Write { contents } => {
                println!("{}: {}", peer.client_id, contents);
                self.log.push(contents.clone());
                peer.wrap_response(ServerResponse::WriteOk {
                    contents,
                    index: self.log.len() - 1,
                })
            }
        };
        peer.last_response = Some(response.clone());
        peer.next_sequence_num += 1;
        Some(response)
    }
}
